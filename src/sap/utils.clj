(ns sap.utils
  (:require
    [babashka.process :refer [process]]
    [clojure.java.shell :refer [sh]]
    [clojure.string :as str]))


(def ^:dynamic *verbose* nil)


(defn exit
  ([status]
   (System/exit status))
  ([status msg]
   (println msg)
   (exit status)))


(def fail (partial exit 1))


(defn run-sh
  [& args]
  (when *verbose*
    (println (str/join " " args)))
  (let [{:keys [out exit err]} (apply sh args)]
    (if (zero? exit)
      out
      (fail (format
              "Failed to execute command:\n \"%s\"\nError:\n %s"
              (str/join " " args) err)))))


(defn run-proc
  [& args]
  (when *verbose*
    (println (str/join " " args)))
  @(process args {:out :inherit})
  nil)


(defn duration
  [start end]
  (let [diff   (java.time.Duration/between end start)
        units  [[(.toDays diff) "d"]
                [(mod (.toHours diff) 24) "h"]
                [(mod (.toMinutes diff) 60) "m"]
                [(mod (.toSeconds diff) 60) "s"]]
        result (->> units
                    (filter (fn [[diff _]] (pos? diff)))
                    (map (fn [[diff unit]] (format "%d%s" diff unit)))
                    (str/join))]
    result))


(defn now
  []
  (java.time.Instant/now))


(defn ->inst
  [s]
  (java.time.Instant/parse s))


(defn print-rows
  [rows]
  (when (seq rows)
    (let [divider (apply str (repeat 4 " "))
          cols    (keys (first rows))
          headers (map #(str/upper-case (name %)) cols)
          widths  (map
                    (fn [k]
                      (apply max (count (str k)) (map #(count (str (get % k))) rows)))
                    cols)
          fmts    (map #(str "%-" % "s") widths)
          fmt-row (fn [row]
                    (apply str
                           (interpose divider
                                      (for [[col fmt]
                                            (map vector (map #(get row %) cols) fmts)]
                                        (format fmt (str col))))))]
      (println (fmt-row (zipmap cols headers)))
      (doseq [row rows]
        (println (fmt-row row))))))

