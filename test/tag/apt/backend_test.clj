(ns tag.apt.backend-test
  (:import (uk.ac.susx.tag.apt PersistentKVStore))
  (require [clojure.test :refer :all]
           [clojure.java.io :as io]))

(def ^:dynamic *test-file* (io/as-file "giga-conll/nyt_cna_eng_201012conll.gz"))


