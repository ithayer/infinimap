(ns infinimap.core)
;; From: http://daly.axiom-developer.org/clojure.pdf


(defprotocol INode 
  "Is an internal node."
  (get-left  [_])
  (get-right [_])

  (add-left  [_ n])
  (add-right [_ n])

  (del-left  [_ n])
  (del-right [_ n])
  
  (balance-left  [_ n])
  (balance-right [_ n])

  (redden    [_] "Makes red.")
  (blacken   [_] "Makes black.")

  (get-key   [_])
  (get-val   [_])
  
  (replace   [_ key val left right]))

(declare make-red)
(declare make-black)

(declare balance-left-del)
(declare balance-right-del)

(defn balance-left-all [this p]
  (make-black (get-key p) (get-val p) this (get-right p)))

(defn balance-right-all [this p]
  (make-black (get-key p) (get-val p) (get-left p) this))

(deftype BlackNode [key val left right]
  INode
  (get-left  [_] left)
  (get-right [_] right)

  (add-left  [this n] (balance-left n this))
  (add-right [this n] (balance-right n this))

  (del-left  [this n] (balance-left-del  key val n    right))
  (del-right [this n] (balance-right-del key val left n))

  (balance-left  [this p] (balance-left-all this p))
  (balance-right [this p] (balance-right-all this p))

  (redden    [_]    (make-red key val left right))
  (blacken   [this] this)
  
  (get-key   [_] key)
  (get-val   [_] val)

  (replace   [this key* val* left* right*] (BlackNode. key* val* left* right*)))

(defn make-black [key val left right]
  (BlackNode. key val left right))

(deftype RedNode [key val left right]
  INode
  (get-left  [_] left)
  (get-right [_] right)

  (add-left  [this n] (RedNode. key val n right))
  (add-right [this n] (RedNode. key val left n))

  (del-left  [this n] (RedNode. key val n right))
  (del-right [this n] (RedNode. key val left n))

  (balance-left  [this p] 
    (if (nil? val)
      (cond
       (instance? RedNode left)
       (RedNode. key val (blacken left)
                 (BlackNode. (get-key p) (get-val p) right (get-right p)))
       (instance? RedNode right)
       (RedNode. (get-key right) (get-val right) 
                 (BlackNode. key val left (get-left right))
                 (BlackNode. (get-key p) (get-val p)
                             (get-right right) (get-right p)))
       :otherwise
       (balance-left-all this p))))

  (balance-right [this p] 
    (if (nil? val)
      (cond
       (instance? RedNode right)
       (RedNode. key val 
                 (BlackNode. (get-key p) (get-val p)
                             (get-left p) left)
                 (blacken right))
       (instance? RedNode left)
       (RedNode. key val
                 (BlackNode. (get-key p) (get-val p)
                             (get-left p) (get-left left))
                 (BlackNode. key val (get-right left) right))
       :otherwise
       (balance-right-all this p))))

  (redden    [_]    (throw (Exception. (str "Tried to redden red node: " key val left right))))
  (blacken   [this] (BlackNode. key val left right))
  
  (get-key   [_] key)
  (get-val   [_] val)

  (replace   [this key* val* left* right*] (RedNode. key* val* left* right*)))

(defn make-red [key val left right]
  (RedNode. key val left right))

(defn do-left-balance [key val n right]
  (cond 
   (and (instance? RedNode n)
        (instance? RedNode (get-left n)))
   (RedNode. (get-key n) (get-val n) (-> n get-left blacken)
             (BlackNode. key val (get-right n) right))
   
   (and (instance? RedNode n)
        (instance? RedNode (get-right n)))
   (RedNode. (-> n get-right get-key) (-> n get-right get-val)
             (BlackNode. (get-key n) (get-val n)
                         (get-left n) (-> n get-right get-left))
             (BlackNode. key val (-> n get-right get-right) right))
   
   :otherwise
   (BlackNode. key val n right)))

(defn balance-right-del [key val left del]
  (cond
   (instance? RedNode del)
   (RedNode. key val left (blacken del))

   (instance? BlackNode left)
   (do-left-balance key val (redden left) del)

   (and (instance? RedNode left)
        (instance? BlackNode (get-right left)))
   (let [lr (-> left get-right)]
     (RedNode. (get-key lr) 
               (get-val lr)
               (do-left-balance (get-key left) (get-val left)
                                (-> left get-left redden)
                                (-> left get-right get-left))
               (BlackNode. key val (-> lr get-right) del)))

   :otherwise
   (throw (Exception. (str "Couldn't balance-right-del: " key val left del)))))

(defn do-right-balance [key val left n]
  (cond
   (and (instance? RedNode n)
        (instance? RedNode (get-right n)))
   (RedNode. (get-key n) (get-val n)
             (BlackNode. key val left (get-left n))
             (-> n get-right blacken))
   
   (and (instance? RedNode n)
        (instance? RedNode (get-left n)))
   (RedNode. (-> n get-left get-key) (-> n get-left get-val)
             (BlackNode. key val left (-> n get-left get-left))
             (BlackNode. (get-key n) (get-val n)
                         (-> n get-left get-right) (get-right n)))
   
   :otherwise
   (BlackNode. key val left n)))

(defn balance-left-del [key val del right]
  (cond
   (instance? RedNode del)
   (RedNode. key val (blacken del) right)
   
   (instance? BlackNode right)
   (do-right-balance key val del (redden right))
   
   (and (instance? RedNode right)
        (instance? BlackNode (get-left right)))
   (let [rl (-> right get-left)]
     (RedNode. (get-key rl) (get-val rl)
               (BlackNode. key val del (-> rl get-left))
               (do-right-balance (get-key right) (get-val right)
                                 (-> rl get-right)
                                 (-> right get-right redden))))
   :otherwise
   (throw (Exception. (str "Couldn't balance-left-del: " key val del right)))))
  


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
 