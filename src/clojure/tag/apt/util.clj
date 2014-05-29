(ns tag.apt.util)

(defn pmapall
  "like pmap, but eager, and with more efficient core usage."
  [f coll]
  (let [num_threads (.. Runtime getRuntime availableProcessors)
        remaining   (atom ())
        func        (fn [item]
                      (let [result (f item)]
                        (swap! remaining next)
                        result))
        futures     (map #(future (func %)) coll)]
    (reset! remaining (drop num_threads futures))
    (first @remaining) ; run the futures we just dropped + 1 more
    (map deref futures)))

(defn pmapall-chunked
  "like pmap-chunked, but with pmapall"
  [n f coll]
  (apply concat
         (pmapall #(doall (map f %)) (partition-all n coll))))
