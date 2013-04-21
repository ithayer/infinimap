;; For this first version, the meta-data is stored in-process.  If we
;; were to rely on the metadata stored in redis, then we'd need to do
;; reference counting there. We could probably do it pretty
;; efficiently in lua, but keeping it simple for now.

(ns infinimap.redis
  (:refer-clojure :exclude [read get empty set! copy! mark! sweep!])
  (:require [clojure.string :as string]
            [taoensso.carmine :as car]))

(def pool         (car/make-conn-pool)) 
(def spec-server1 (car/make-conn-spec)) 

(defmacro wcar [& body] `(car/with-conn pool spec-server1 ~@body))

(defprotocol IDataNode
  ;; An IDataNode can actually store elements.
  (store  [n k v])
  (delete [n k])
  (lookup [n k])
  (size   [n])
  (data   [n]))

(defprotocol INodeInfoStore
  (empty  [s]
    "This returns the canonical id of the empty element.")
  (generate-id [s]
    "Generates a unique id.")
  (put!   [s id k v]
    "Set the key and value in node 'id'. Returns true if element existed, false otherwise.")
  (get    [s id k]
    "Get the value of key k in node 'id'.")
  (create [s id m]
    "Create a new key/value map for node 'id'.")
  (read   [s id]
    "Gets the entire key/value map for node 'id'.")
  (copy!  [s id]
    "Copies node with 'id'. Returns the new id.")
  (mark!  [s ids]
    "Marks the nodes with ids 'ids' as used. We can
     use redis sorted set to find ones that haven't
     been touched recently.") 
  (sweep! [s]
    "Clean up old nodes."))

(defn uuid [] (->> (java.util.UUID/randomUUID)
                   (.toString)
                   (remove #{\-})
                   (apply str)))

(defn swap!-exists?
  [store location v]
  (dosync 
   (let [nf (gensym)
         ret (get-in @store location nf)]
     (alter store assoc-in location v)
     (= nf ret))))

(deftype MemoryNodeInfoStore [options]
  INodeInfoStore
  (empty  [s] 0)
  (generate-id [s]
    (uuid))
  (put!   [s id k v]
    (-> options :store (swap!-exists? [id k] v)))
  (get    [s id k]
    (-> options :store deref (clojure.core/get-in [id k])))
  (create [s id m]
    (dosync (alter (-> options :store) assoc id m)))
  (read   [s id]
    (-> options :store deref (clojure.core/get id)))
  (copy!  [s id]
    (let [x (-> options :store deref (clojure.core/get id))
          new-id (uuid)]
      (dosync (-> options :store (commute assoc new-id x)))
      new-id))
  (mark!  [s ids]
    (doseq [id ids]
      (dosync (-> options :store (commute assoc-in [:marks id] (System/currentTimeMillis))))))
  (sweep! [s]
    (let [old (->> (-> options :store deref) :marks
                   (filter #(-> % second (< (- (System/currentTimeMillis) 60000))))
                   (map first))]
      (println (format "Sweeping up %d old keys" (count old)))
      (doseq [x old]
        (dosync
         (let [marks (-> options :store (commute dissoc x) deref :marks)]
           (-> options :store (alter assoc :marks (dissoc marks x))))))
      (println (format "%d keys left" (-> options :store deref count))))))

(def node-storage-size 4)
(def node-allocation-size (* 2 node-storage-size))
(def node-storage-mask 0x3)

(defn compute-level-hash [k level]
  (-> k
      hash
      (bit-shift-right (bit-shift-left level 2)) 
      (bit-and node-storage-mask)))

(declare make-data-node)

(deftype InnerNode [m data]
  IDataNode
  (store [_ k v]
    (println "Store not implemented for inner node.")))


;; TODO: find a way to efficiently copy data elements on the store.
(defn make-inner-node
  [m storage]
  ;; !! LEFT HERE -- right now an inner node stores its data in-process.

  ;; -- but what is it's data? it's data is DataNodes or
  ;; InnerNodes. 
  ;;
  ;; Inner Nodes' data is a map of level-hash -> DataNode or
  ;; InnerNode.  It doesn't make sense to make this a map of
  ;; level-hash -> id unless complete nodes are stored on the store
  ;; (because then we'd need to go look up the id, then find it in the
  ;; process' hash). And if it's not a map of level-hash -> id, then
  ;; there's nothing else it can be, the inner nodes would need to be
  ;; in-process only.

  ;; And if we kept inner nodes inside the process then we'd not be
  ;; able to share it across other machines. So, we should store full
  ;; node contents on hte store. That means to do proper GC, we'd need
  ;; to do reference counting on the store.
                                      
  (println "Making inner node:  m")
  (let [new-id (generate-id storage)
        existing (read storage (-> m :id))
        new-data (->> existing
                      (group-by #(-> % first (compute-level-hash (-> m :level))))
                      (map (fn [[hash-val data]]
                             [hash-val (make-data-node (-> m :level inc) data storage)]))
                      (into {}))]
    (println "Creating storage cell :" new-id new-data)
    (InnerNode. {:id new-id
                 :level (-> m :level)
                 :size (->> existing count)}
                new-data)))

(deftype DataNode [m storage]
  IDataNode
  (store [_ k v]
    (if (-> m :size (= (dec node-storage-size)))
      (make-inner-node m storage)
      ;; Otherwise, we check if it exists to avoid making an unneeded copy.
      (let [new-id (copy! storage (-> m :id))
            new-key? (put! storage new-id k v)]
        (DataNode. {:id new-id
                    :level (:level m)
                    :size (if new-key? (inc (:size m)) (:size m))}
                   storage)))))

(defn make-data-node [level items storage]
  (let [new-id (generate-id storage)]
    (DataNode. {:id new-id
                :level level
                :size (->> items count)}
               storage)))

(deftype EmptyDataNode [storage]
  IDataNode
  (store [_ k v]
    (let [new-id (copy! storage (empty storage))]
      (put! storage new-id k v)
      (DataNode. {:level 0
                  :size  1
                  :id new-id}
                 storage)))
  (size [_] 0))
