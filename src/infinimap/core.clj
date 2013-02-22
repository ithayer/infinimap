(ns infinimap.core)

;; Black - a black leaf node with a null value
;; BlackVal - a black leaf node with a value
;; BlackBranch - a black interior node with children and a null value
;; BlackBranchVal - a black interior node with children and a value
;; Red - a red leaf node with a null value
;; RedVal - a red leaf node with a value
;; RedBranch - a red interior node with children and a null value
;; RedBranchVal - a red interior node with children and a value

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

(defn balance-left-del [a b c d])
(defn balance-right-del [a b c d])

(declare make-red)
(declare make-black)

(defn balance-left-all [this p]
  (make-black (get-key p) (get-val p) this (get-right p)))

(defn balance-right-all [this p]
  (make-black (get-key p) (get-val p) (get-left p) this))

(deftype BlackNode [key val left right]
  ;; A black leaf node with a null value.
  INode
  (get-left  [_] left)
  (get-right [_] right)

  (add-left  [this n] (balance-left n this))
  (add-right [this n] (balance-right n this))

  (del-left  [this n] (balance-left-del  key val n    right))
  (del-right [this n] (balance-right-del key val left n))

  (balance-left  [this p] (balance-left-all this p))
  (balance-right [this p] (balance-right-all this p))

  (redden    [_]    (make-red key nil nil nil))
  (blacken   [this] this)
  
  (get-key   [_] key)
  (get-val   [_] val)

  (replace   [this key* val* left* right*] (BlackNode. key* val* left* right*)))

(deftype BlackVal [key val left right]
  ;; A black leaf node with a value.
  INode
  (get-left  [_] left)
  (get-right [_] right)

  (add-left  [this n] (balance-left n this))
  (add-right [this n] (balance-right n this))

  (del-left  [this n] (balance-left-del  key val n    right))
  (del-right [this n] (balance-right-del key val left n))

  (balance-left  [this p] (balance-left-all this p))
  (balance-right [this p] (balance-right-all this p))

  (redden    [_]    (make-red key val nil nil))
  (blacken   [this] this)
  
  (get-key   [_] key)
  (get-val   [_] val)

  (replace   [this key* val* left* right*] (BlackNode. key* val* left* right*)))

(deftype BlackBranch [key val left right]
  ;; A black interior node with children and a null value
  INode
  (get-left  [_] left)
  (get-right [_] right)

  (add-left  [this n] (balance-left n this))
  (add-right [this n] (balance-right n this))

  (del-left  [this n] (balance-left-del  key val n    right))
  (del-right [this n] (balance-right-del key val left n))

  (balance-left  [this p] (balance-left-all this p))
  (balance-right [this p] (balance-right-all this p))

  (redden    [_]    (make-red key nil left right))
  (blacken   [this] this)
  
  (get-key   [_] key)
  (get-val   [_] val)

  (replace   [this key* val* left* right*] (BlackNode. key* val* left* right*)))

(deftype BlackBranchVal [key val left right]
  ;; A black interior node with children and a value.
  INode
  (get-left  [_] left)
  (get-right [_] right)

  (add-left  [this n] (balance-left n this))
  (add-right [this n] (balance-right n this))

  (del-left  [this n] (balance-left-del  key val n    right))
  (del-right [this n] (balance-right-del key val left n))

  (balance-left  [this p] (balance-left-all this p))
  (balance-right [this p] (balance-right-all this p))

  (redden    [_]    (make-red key nil left right))
  (blacken   [this] this)
  
  (get-key   [_] key)
  (get-val   [_] val)

  (replace   [this key* val* left* right*] (BlackNode. key* val* left* right*)))



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
 