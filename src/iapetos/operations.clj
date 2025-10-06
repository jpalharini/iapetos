(ns iapetos.operations
  (:require [iapetos.collector :as collector])
  (:import [io.prometheus.client
            Counter$Child
            Histogram$Child
            Histogram$Timer
            Gauge$Child
            Gauge$Timer
            Summary$Child
            Summary$Timer]
           [io.prometheus.metrics.core.datapoints DistributionDataPoint Timer TimerApi]
           [io.prometheus.metrics.core.metrics Counter$DataPoint Gauge$DataPoint Histogram$DataPoint StatefulMetric Summary Summary$DataPoint]
           [io.prometheus.metrics.model.snapshots ClassicHistogramBucket ClassicHistogramBuckets DataPointSnapshot DistributionDataPointSnapshot HistogramSnapshot$HistogramDataPointSnapshot Labels MetricSnapshot]
           [java.util List]))

(defn- get-latest-distribution-snapshot [{:keys [register collector]} labels]
  (let [instance   ^StatefulMetric @register
        snapshot   ^MetricSnapshot (.collect instance)
        reg-labels ^"[Ljava.lang.String;" (into-array (:labels collector))
        labels-obj ^Labels (Labels/of reg-labels (collector/ordered-labels reg-labels labels))]
    (loop [datapoints (.getDataPoints snapshot)]
      (when-let [curr-datapoint ^DataPointSnapshot (first datapoints)]
        (if (= (.getLabels curr-datapoint) labels-obj)
          curr-datapoint
          (recur (rest datapoints)))))))

(defn- start-timer* [^TimerApi datapoint]
  (let [^Timer t (.startTimer ^TimerApi datapoint)]
    #(.observeDuration t)))

;; ## Operation Protocols

(defprotocol ReadableCollector
  (read-value [this]))

(defprotocol IncrementableCollector
  (increment* [this amount]))

(defprotocol DecrementableCollector
  (decrement* [this amount]))

(defprotocol SettableCollector
  (set-value [this value])
  (set-value-to-current-time [this]))

(defprotocol ObservableCollector
  (observe [this amount]))

(defprotocol TimeableCollector
  (start-timer [this]))

;; ## Derived Functions

(defn increment
  ([this] (increment this 1.0))
  ([this amount] (increment* this amount)))

(defn decrement
  ([this] (decrement this 1.0))
  ([this amount] (decrement* this amount)))

;; ## Counter

(extend-type Counter$DataPoint
  ReadableCollector
  (read-value [this]
    (.get ^Counter$DataPoint this))
  IncrementableCollector
  (increment* [this amount]
    (.inc ^Counter$DataPoint this (double amount))))

;; ## Gauge

(extend-type Gauge$DataPoint
  ReadableCollector
  (read-value [this]
    (.get ^Gauge$DataPoint this))

  IncrementableCollector
  (increment* [this amount]
    (.inc ^Gauge$DataPoint this (double amount)))

  DecrementableCollector
  (decrement* [this amount]
    (.dec ^Gauge$DataPoint this (double amount)))

  ObservableCollector
  (observe [this amount]
    (.set ^Gauge$DataPoint this (double amount)))

  SettableCollector
  (set-value [this value]
    (.set ^Gauge$DataPoint this (double value)))
  (set-value-to-current-time [this]
    (let [unix-time (/ (System/currentTimeMillis) 1000.0)]
      (.set ^Gauge$DataPoint this unix-time)))

  TimeableCollector
  (start-timer [this]
    (start-timer* this)))

;; ## Histogram

(defn- buckets->vec
  [^HistogramSnapshot$HistogramDataPointSnapshot snapshot]
  (let [buckets ^ClassicHistogramBuckets (.getClassicBuckets snapshot)]
    (->> buckets (.iterator) (iterator-seq)
         (mapv #(.getCount ^ClassicHistogramBucket %)))))

(extend-type Histogram$DataPoint
  ObservableCollector
  (observe [this amount]
    (.observe ^Histogram$DataPoint this (double amount)))

  TimeableCollector
  (start-timer [this]
    (start-timer* this)))

;; ## Summary

(extend-type Summary$DataPoint
  ObservableCollector
  (observe [this amount]
    (.observe ^Summary$DataPoint this (double amount)))

  TimeableCollector
  (start-timer [this]
    (start-timer* this)))

(defn read-distribution-value [metric labels]
  (when-let [snapshot ^DistributionDataPointSnapshot (get-latest-distribution-snapshot metric labels)]
    (let [type (-> metric :collector :type)]
      (cond-> {:count (.getCount snapshot)
               :sum   (.getSum snapshot)}
              (= type :histogram) (assoc :buckets (buckets->vec snapshot))
              ; todo: implement similar logic to find quantiles - needs to be a map!
              (= type :summary) (assoc :quantiles nil)))))