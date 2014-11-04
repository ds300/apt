(ns tag.apt.ppmi
  (:import (uk.ac.susx.tag.apt APTFactory APTVisitor))
  (:import (uk.ac.susx.tag.apt DistributionalLexicon BidirectionalIndexer APT ArrayAPT APTFactory)))

(defn from-path [apt [r & more :as path]]
  (if r
    (if-let [rapt (.getChild apt r)]
      (from-path rapt more)
      (from-path (.withEdge apt r (.empty ArrayAPT/factory)) path))
    apt))

(defn reverse-path [path]
  (int-array (reverse (map (partial * -1) path))))

(defn freq2ppmi [^DistributionalLexicon input-lexicon output-lexicon]
  (let [total-count (.getSum input-lexicon)
        entities (map int (.getIndices (.getEntityIndex input-lexicon)))]
    (doseq [e entities]
      (let [count-apt (.get input-lexicon e)
            count (.sum count-apt)
            ppmi-root-apt (atom (.empty ArrayAPT/factory))]
        (.walk count-apt (reify APTVisitor
                           (visit [_ path apt]
                             (loop [ppmi-apt (from-path @ppmi-root-apt path)
                                    [[label label-count] & more] (seq (.entrySet (.entityScores apt)))]
                               (if label
                                 (let [label-apt (.get input-lexicon label)
                                       path2label-count (let [kid (.getChildAt label-apt (reverse-path path))]
                                                          (if (nil? kid)
                                                            (do
                                                              (.print label-apt)
                                                              (throw (RuntimeException. (str "bad: " label " " (vec path) " " (vec (reverse-path path))))))
                                                            (.sum kid)))
                                       prob-path2label (double (/ path2label-count total-count))
                                       prob-path2label|e (double (/ label-count count))
                                       pmi (Math/log (double (/ prob-path2label|e prob-path2label)))]
                                   (if (> pmi 0)
                                     (recur (.withScore ppmi-apt label (float pmi)) more)
                                     (recur ppmi-apt more)))
                                 (reset! ppmi-root-apt (from-path ppmi-apt (reverse-path path))))))))
        (.include output-lexicon e @ppmi-root-apt)))))



