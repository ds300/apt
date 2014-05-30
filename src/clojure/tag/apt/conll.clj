(ns tag.apt.conll
  (:import (java.io StringReader BufferedReader Reader)))


(defmacro or= [val & vals]
  `(or ~@(for [v vals] `(= ~val ~v))))

(defn- parse* [rdr acc]
  (let [c (.read rdr)]
    (cond
      (= c 10)    (if (zero? (count acc))
                    (parse* rdr acc)
                    (cons (persistent! acc) (lazy-seq (parse* rdr (transient [])))))

      (not= c -1) (loop [c c line (transient []) s (StringBuilder.)]
                    (cond
                      (or= c 10 -1) (let [item (.toString s)
                                          line (conj! line item)]
                                      (if (and (= 1 (count line)) (= item ""))
                                        (parse* rdr acc)
                                        (parse* rdr (conj! acc (persistent! line)))))

                      (= c 9)       (recur (.read rdr) (conj! line (.toString s)) (StringBuilder.))

                      :else         (recur (.read rdr) line (.append s (char c)))))

      :else       (do (.close rdr)
                      (when-not (zero? (count acc))
                        (cons (persistent! acc) nil))))))

(defn- fnser [fns]
  (fn [v]
    (mapv (fn [f val] (f val)) fns v)))

(defn parse
  ([^Reader rdr]
    (parse* rdr (transient [])))
  ([^Reader rdr fns]
    (map (partial mapv (fnser fns)) (parse rdr))))

