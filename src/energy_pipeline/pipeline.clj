(ns energy-pipeline.pipeline
    (:require
     [grafter.tabular :refer [_ add-column add-columns apply-columns
                              build-lookup-table column-names columns
                              derive-column drop-rows graph-fn grep make-dataset
                              mapc melt move-first-row-to-header read-dataset
                              read-datasets rows swap swap take-rows
                              test-dataset test-dataset write-dataset]]
     [grafter.rdf :refer [s]]
     [grafter.rdf.protocols :refer [->Quad]]
     [grafter.rdf.templater :refer [graph]]
     [grafter.pipeline :refer [declare-pipeline]]
     [grafter.vocabularies.rdf :refer :all]
     [grafter.vocabularies.foaf :refer :all]
     [energy-pipeline.prefix :refer [base-id base-graph base-vocab base-data]]
     [energy-pipeline.transform :refer [->integer]]
     [clojure.java.io :refer :all]
     [clojure.data.csv :as csv]))

(def row-headers ["H" "OPTIMA" "Half" "Hourly" "DATA" "READING" "DATE" "CHANNEL" "TYPE" "UNITS" "00:30" "01:00" "01:30" "02:00" "02:30" "03:00" "03:30" "04:00" "04:30" "05:00" "05:30" "06:00" "06:30" "07:00" "07:30" "08:00" "08:30" "09:00" "09:30" "10:00" "10:30" "11:00" "11:30" "12:00" "12:30" "13:00" "13:30" "14:00" "14:30" "15:00" "15:30" "16:00" "16:30" "17:00" "17:30" "18:00" "18:30" "19:00" "19:30" "20:00" "20:30" "21:00" "21:30" "22:00" "22:30" "23:00" "23:30" "24:00" "Daily" "Total" "Max" "Reading" "Min" "Reading" "Data" "Source" "Status" "Site" "Name"])

(def example-dir-location "/Users/LynhAl/Downloads/FoodStoreMeters_1ASGas_fixed")
(def example-dir-2 "/Users/LynhAl/Downloads/Elec")
(def example-out-location "/Users/LynhAl/Documents/elec.csv")
(def example-dir-small "/Users/LynhAl/Downloads/Elec-small")

(comment (with-open [writer (io/writer example-out-location)]
           (csv/write-csv writer
                          [])))

(defn walk [dirpath pattern]
  (doall (filter #(re-matches pattern (.getName %))
                 (file-seq (file dirpath)))))

(defn get-list-of-file-paths [data-dir pattern]
  (map #(-> % .getPath str) (file-seq (file data-dir)))

  (-> data-dir
      file
      file-seq
      (->>
       (map #(-> % .getPath str))
       (filter #(re-find pattern %)))))

(def get-rows
  (fn [file-path]
    (-> (read-dataset file-path :format :xls)
        (drop-rows 1)
        :rows)))

(defn sanitize-name-column [row]
  (if-let [name (or (get row "Name")
                    (get row "Site Name")
                    (get row "bh"))]

    (let [cedar-id (-> name
                       (clojure.string/replace #"[^0-9]" "")
                       clojure.string/trim)
          
          new-row (-> row
                      (assoc "cedar-id" cedar-id)
                      (assoc "bh" cedar-id))]
      new-row
      )))

(comment (or (get test-second-row "Name")
             (get test-second-row "Site Name")
             (get test-second-row "bh")))

(defn write-dataset-to-csv [rows-seq out-location]
  (let [sanitized-rows (->> (map sanitize-name-column rows-seq)
                            (filter identity))
        ds (make-dataset sanitized-rows)]
    (write-dataset out-location
                   ds
                   :format :csv)))

(defn convert-excel-to-csv [data-dir out-location]
  (let [file-locations (get-list-of-file-paths data-dir #"\.xlsx$")
        rows-seq (->> (concat (map get-rows file-locations))
                      (apply concat))]
    (write-dataset-to-csv rows-seq out-location)))

;; (convert-excel-to-csv example-dir-location example-out-location)

(defn excel-csv-convert [data-dir]
  (let [file-locations (get-list-of-file-paths data-dir #"\.xlsx$")]
    (doseq [file file-locations]
      (let [out-file-path (clojure.string/replace file #"\.xlsx$" ".csv")]
        (-> (get-rows file)
            (write-dataset-to-csv out-file-path))))))

(comment (doseq [file example-locs]
           (let [out-file-path (clojure.string/replace file #"\.xlsx$" ".csv")]
             (println out-file-path))))

;; (exel-csv-convert example-dir-2)

(defn copy-file [source-path dest-path]
  (copy (file source-path) (file dest-path)))

(defn modify-file-format []
  (map (fn [file-path]
         (let [new-file-path (-> file-path
                                 (clojure.string/replace #"XLSX" "xlsx"))]
           (copy-file file-path new-file-path)))
       (get-list-of-file-paths example-dir-location #"\.XLSX$")))
