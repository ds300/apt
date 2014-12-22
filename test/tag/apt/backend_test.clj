(ns tag.apt.backend-test
  (require [tag.apt.backend :as b]
           [clojure.test :refer :all]
           [clojure.java.io :as io]))

(def ^:dynamic *test-file* (io/as-file "giga-conll/nyt_cna_eng_201012conll.gz"))


