(ns sap.utils
  (:require
   [clojure.string :as str]))

(defn exit
  [status msg]
  (println msg)
  (System/exit status))


(defn now
  []
  (java.time.Instant/now))


(defn ->inst
  [s]
  (java.time.Instant/parse s))


(defn at-utc
  [inst]
  (.atZone inst java.time.ZoneOffset/UTC))


(defn duration
  ([end]
   (duration (now) (->inst end)))
  ([start end]
   (let [diff   (java.time.Duration/between end start)
         units  [[(.toDays diff) "d"]
                 [(mod (.toHours diff) 24) "h"]
                 [(mod (.toMinutes diff) 60) "m"]
                 [(mod (.toSeconds diff) 60) "s"]]
         result (->> units
                     (filter (fn [[diff _]] (pos? diff)))
                     (map (fn [[diff unit]] (format "%d%s" diff unit)))
                     (str/join))]
     result)))
