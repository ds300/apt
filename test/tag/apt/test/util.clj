(ns tag.apt.test.util
  (:import (uk.ac.susx.tag.apt.store APTStore$Builder APTStore PersistentKVStore)
           (uk.ac.susx.tag.apt APTFactory Resolver Indexer APT)
           (java.util Arrays)
           (clojure.lang IFn IDeref)
           (java.io ByteArrayOutputStream ByteArrayInputStream)
           (java.util.zip GZIPOutputStream GZIPInputStream))
  (:require [clojure.test :refer :all]))

(defn serialize [^APT apt]
  (with-open [out (ByteArrayOutputStream.)]
    (.writeTo apt out)
    (.toByteArray out)))

(defn deserialize [^bytes bytes ^APTFactory factory]
  (with-open [in (ByteArrayInputStream. bytes)]
    (.read factory in)))

(defn gz-serialize [^APT apt]
  (with-open [baos (ByteArrayOutputStream.) ]
    (with-open [out (GZIPOutputStream. baos)]
      (.writeTo apt out))
    (.toByteArray baos)))

(defn gz-deserialize [^bytes bytes ^APTFactory factory]
  (with-open [in (GZIPInputStream. (ByteArrayInputStream. bytes))]
    (.read factory in)))

(defn reserialize [^APT apt factory]
  (deserialize (serialize apt) factory))

(def readable (comp (partial mapv vec) (partial partition-all 4)))

(defn in-memory-apt-store-builder []
  (let [store (atom nil)]

    (reify
      APTStore$Builder
      (^APTStore build [this ^APTFactory factory]
        (let [trees (atom {})]
          (reset! store (reify
                          APTStore
                          (get [this key]
                            (or (@trees key)
                                (let [new-tree (.empty factory)]
                                  (.put this key new-tree)
                                  new-tree)))
                          (has [this key] (contains? @trees key))
                          (put [this key val] (swap! trees assoc key val))
                          (close [this])
                          IDeref
                          (deref [this] @trees)))))
      IDeref
      (deref [this] @store))))

(defn in-memory-caching-apt-store-builder []
  (let [store (atom nil)]

    (reify
      APTStore$Builder
      (^APTStore build [this ^APTFactory factory]
        (let [trees (atom {})]
          (reset! store (reify
                          APTStore
                          (get [this key]
                            (deserialize (or (@trees key)
                                             (let [new-tree (serialize (.empty factory))]
                                               (swap! trees assoc key new-tree)
                                               new-tree))
                                         factory))
                          (has [this key] (contains? @trees key))
                          (put [this key val] (swap! trees assoc key (serialize val)))
                          (close [this])
                          IDeref
                          (deref [this] @trees)))))
      IDeref
      (deref [this] @store))))

(defn in-memory-gzip-caching-apt-store-builder []
  (let [store (atom nil)]

    (reify
      APTStore$Builder
      (^APTStore build [this ^APTFactory factory]
        (let [trees (atom {})]
          (reset! store (reify
                          APTStore
                          (get [this key]
                            (gz-deserialize (or (@trees key)
                                             (let [new-tree (gz-serialize (.empty factory))]
                                               (swap! trees assoc key new-tree)
                                               new-tree))
                                         factory))
                          (has [this key] (contains? @trees key))
                          (put [this key val] (swap! trees assoc key (gz-serialize val)))
                          (close [this])
                          IDeref
                          (deref [this] @trees)))))
      IDeref
      (deref [this] @store))))

(deftype BytesKey [bytes]
  Object
  (hashCode [this] (Arrays/hashCode bytes))
  (equals [this other] (Arrays/equals bytes (.bytes ^BytesKey other))))

(deftest bytes-key-test
  (let [key1 (BytesKey. (byte-array [1 2 3]))
        key2 (BytesKey. (byte-array (mapv inc (range 3))))
        key3 (BytesKey. (byte-array [1 2 4]))]
    (is (= key1 key2))
    (is (= (.hashCode key1) (.hashCode key2)))
    (is (not= key1 key3))
    (is (not= key2 key3))))

(defn in-memory-byte-store []
  (let [store (atom {})]
    (reify PersistentKVStore
      (store [this k v] (swap! store assoc (BytesKey. k) v))
      (get [this k] (@store (BytesKey. k)))
      (contains [this k] (contains? @store (BytesKey. k)))
      (close [this]))))

(deftest byte-store-test
  (let [store (in-memory-byte-store)]
    (.store store (byte-array [1 2 3]) "hi there") ; vals are intended to be byte arrays too, but it doesn't matter for testing
    (is (= "hi there" (.get store (byte-array (mapv inc (range 3))))))
    (.store store (byte-array [1 2 3]) "hit here")
    (is (= "hit here" (.get store (byte-array (mapv inc (range 3))))))))


(defn indexer [start]
  (let [state (atom {:next start :val2int {} :int2val {}})]
    (reify
      Indexer
      (getIndex [this val]
        (let [current @state]
          (if-let [i (get-in current [:val2int val])]
            i
            (let [new-state (assoc current :next (inc (:next current))
                                           :val2int (assoc (:val2int current) val (:next current))
                                           :int2val (assoc (:int2val current) (:next current) val))]
              (if (compare-and-set! state current new-state)
                (:next current)
                (recur val))))))
      (hasIndex [this val]
        (boolean (get-in @state [:val2int val])))

      Resolver
      (resolve [this i]
        (get-in @state [:int2val i]))

      IFn
      (invoke [this val]
        (if (or (instance? Long val) (instance? Integer val))
          (.resolve this val)
          (.getIndex this val))))))

(deftest indexer-test
  (let [idx (indexer 0)]
    (is (= 0 (idx "cheese") (idx "cheese") (idx "cheese")))
    (is (= 1 (idx "doge") (idx "doge") (idx "doge")))
    (is (= 2 (idx "monkey") (idx "monkey") (idx "monkey")))
    (is (not= (idx "doge") (idx "monkey") (idx "cheese")))
    (is (= "cheese" (idx 0)))
    (is (= "doge" (idx 1)))
    (is (= "monkey" (idx 2))))
  (let [idx (indexer 5)]
    (is (= 5 (idx "cheese") (idx "cheese") (idx "cheese")))
    (is (= 6 (idx "doge") (idx "doge") (idx "doge")))
    (is (= 7 (idx "monkey") (idx "monkey") (idx "monkey")))
    (is (not= (idx "doge") (idx "monkey") (idx "cheese")))
    (is (= "cheese" (idx 5)))
    (is (= "doge" (idx 6)))
    (is (= "monkey" (idx 7)))))

(def no-throw-exception (Exception. "Expecting exception to be thrown"))
