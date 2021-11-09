(ns debug
  (:require
    [clojure.string :as str]))


(fn [s]
  (let [end      (count s)
        appender (fn [idx]
                   (let [c (nth s idx)]
                     (if (java.lang.Character/isUpperCase c)
                       (str "-" (str/lower-case c))
                       c)))]
    (loop [curr 0
           acc  ""]
      (if (= end curr)
        acc
        (recur (inc curr) (str acc (appender curr)))))))
