(ns tag.apt.test.store
  (:import (uk.ac.susx.tag.apt LRUCachedAPTStore LRUCachedAPTStore$Builder)
           (uk.ac.susx.tag.apt ArrayAPT$Factory ArrayAPT Util))
  (:require [clojure.test :refer :all]
            [tag.apt.test.util :refer [in-memory-byte-store]]))



