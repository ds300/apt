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

      :else (when-not (zero? (count acc))
              (cons (persistent! acc) nil)))))

(defn- fnser [fns]
  (fn [v]
    (mapv (fn [f val] (f val)) fns v)))

(defn parse
  "Parses conll-formatted data from a java.io.Reader, returning a lazy seq of sentence vectors, where each token in
  a sentence is a vector of string values. If fns is supplied, it should be a vector of functions which are applied to
  the corresponding token fields.

  e.g. given input like

```
0       They    they    PRP     2       nsubj
1       are     be      VBP     2       cop
2       killers killer  NNS     -1      root
3       .       .       .
```

  for `(parse in)` you'll get output like

```
([
  [\"0\" \"They\" \"they\" \"PRP\" \"2\" \"nsubj\"]
  [\"1\" \"are\" \"be\" \"VBP\" \"2\" \"cop\"]
  [\"2\" \"killers\" \"killer\" \"NNS\" \"2\" \"root\"]
  [\"3\" \".\" \".\" \".\"]
])
```

  but for `(parse in [#(Integer. %) str str keyword #(Integer. %) keyword])` you'll get output like

```
([
  [0 \"They\" \"they\" :PRP 2 :nsubj]
  [1 \"are\" \"be\" :VBP 2 :cop]
  [2 \"killers\" \"killer\" :NNS 2 :root]
  [3 \".\" \".\" :.]
])
```

  "
  ([^Reader rdr]
    (parse* rdr (transient [])))
  ([^Reader rdr fns]
    (map (partial mapv (fnser fns)) (parse rdr))))

