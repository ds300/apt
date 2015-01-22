(ns tag.apt.compose)

(defn parse-file [f]
  ())

(defn -main [lex-dir & files]
  (let [sents (mapcat parse-file files)]))