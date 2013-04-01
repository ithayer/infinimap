(ns infinimap.core
  (:require [clojure.string :as string]))

;; Next step: implement store for inner node
;; -- other interface fns
;; -- test/verification
;; -- redis store node type

(defprotocol IDataNode
  ;; An IDataNode can actually store elements.
  (store  [n k v])
  (delete [n k])
  (lookup [n k])
  (size   [n k])
  (data [n]))

(def node-storage-size 4)
(def node-allocation-size (* 2 node-storage-size))
(def node-storage-mask 0x3)

(defn array-add! 
  "Adds an element using chaining. Don't add to a full array, this is unchecked, it will spin."
  ;; TODO: this should probably do strict equality testing on values also.
  ([arr k v]
     (array-add! arr k v (-> k hash (mod node-storage-size) (* 2))))
  ([arr k v idx] ;; Index is relative to node-allocation-size.
     (let [elem (aget arr idx)]
       (if (or (nil? elem) (= elem k))
         (do (println "adding " k "->" v "at" idx ", previously" (aget arr (inc idx)))
             (aset arr idx k)
             (aset arr (inc idx) v)
             (nil? elem)) ;; Return true if newly added.
         (array-add! arr k v (-> idx (+ 2) (mod node-allocation-size)))))))

(deftype InnerNode [m]
  IDataNode
  (store [_ k v]
    (let [h-val (-> k (compute-level-hash (-> m :level)))]
      (.store (aget (-> m :storage) h-val) k v)))
  Object
  (toString [_] 
    (str (-> m (update-in [:storage] #(->> % (map str) (string/join " ")))))))

(declare make-data-node)

(defn to-storage [h-vals]
  (let [a* (make-array Object node-storage-size)]
    (doseq [[h-val data-node] h-vals]
      (aset a* h-val data-node))
    a*))

(defn compute-level-hash [k level]
  (-> k
      hash
      (bit-shift-right (bit-shift-left level 2)) 
      (bit-and node-storage-mask)))

(defn make-inner-node
  "Given a data node, returns an inner node that points to other data nodes."
  [data]
  (let [{:keys [storage size level]} data]
    ;; Partition into hash values depending on the level.
    (InnerNode. {:storage (->> (map vector (take-nth 2 storage) (take-nth 2 (rest storage)))
                               (map (fn [[k v]]
                                      (println "k:"k "v:"v)
                                      (when k
                                        (let [h-val (-> k (compute-level-hash level))]
                                          {h-val [[k v]]}))))
                               (apply merge-with concat)
                               (map (fn [[h-val pairs]]
                                      (println "h:" h-val "p:" pairs)
                                      [h-val (make-data-node (inc level) pairs)]))
                               (to-storage))
                 :size size
                 :level level})))

(deftype DataNode [m]
  IDataNode
  (store [n k v]
    (if (-> m :size (= (dec node-storage-size)))
      (make-inner-node m)
      (let [new-storage (-> m :storage aclone)
            new-key?    (-> new-storage (array-add! k v))]
        (DataNode. {:storage new-storage
                    :level (:level m)
                    :size (if new-key? (inc (:size m)) (:size m))}))))
  (data [_] m)
  Object
  (toString [_] 
    (str (-> m (update-in [:storage] #(->> % (map (fnil str "*nil*")) (string/join " ")))))))

(defn make-data-node [level pairs]
  (DataNode. {:level level
              :size (count pairs)
              :storage (let [a* (make-array Object node-allocation-size)]
                         (println "PAIRS: " pairs)
                         (doseq [[k v] pairs]
                           (array-add! a* k v))
                         a*)}))

(deftype EmptyDataNode []
  IDataNode
  (store [_ k v]
    (let [new-storage (make-array Object node-allocation-size)]
      (array-add! new-storage k v)
      (DataNode. {:storage new-storage
                  :level 0
                  :size  1}))))

;; (deftype DerefMap [m]

;;   clojure.lang.IPersistentMap
;;   (assoc   [_ k v] (DerefMap. (.assoc m k (delay v))))
;;   (assocEx [_ k v] (DerefMap. (.assocEx m k (delay v))))
;;   (without [_ k]   (DerefMap. (.without m k)))

;;   clojure.lang.IPersistentCollection
;;   (count [_] (count m))
;;   (cons [_ o] (let [[k v] o] (DerefMap. (.cons m [k (delay v)]))))
;;   (empty [_] (.empty m))
;;   (equiv [_ o] (and (isa? (class o) DerefMap)
;;                     (.equiv m (.m o))))

;;   clojure.lang.Seqable
;;   (seq [_] (deref-seq (seq m)))
;;   clojure.lang.ILookup
;;   (valAt [_ k] (let [d (.valAt m k)]
;;                  (when-not (nil? d)
;;                    @d)))
;;   (valAt [_ k v] (if (.containsKey m k)
;;                   (deref (.valAt m k))
;;                    v))

;;   clojure.lang.Associative
;;   (containsKey [_ k] (.containsKey m k))
;;   (entryAt [_ k] (let [[k v] (.entryAt m k)]
;;                    [k @v]))

;;   java.lang.Iterable
;;   (iterator [this] (.iterator (seq this))))




;; (defmethod print-method Person [p writer]
;;     (print-person p writer))
 
;;   (defmethod print-dup Person [p writer]
;;     (print-person p writer))
;;  (.addMethod simple-dispatch Person (fn [p]
;;                                        (print-person p *out*))))
 