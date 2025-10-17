(ns iapetos.operations
  (:require [iapetos.collector :as collector])
  (:import [clojure.lang MapEntry]
           [iapetos.collector
            LabeledCallbackCollector
            LabeledDistributionCollector]
           [io.prometheus.metrics.core.datapoints
            DistributionDataPoint
            Timer
            TimerApi]
           [io.prometheus.metrics.core.metrics
            Counter$DataPoint
            Gauge$DataPoint
            MetricWithFixedMetadata]
           [io.prometheus.metrics.model.snapshots
            ClassicHistogramBucket
            ClassicHistogramBuckets
            CounterSnapshot$CounterDataPointSnapshot
            DataPointSnapshot
            DistributionDataPointSnapshot
            GaugeSnapshot$GaugeDataPointSnapshot
            HistogramSnapshot$HistogramDataPointSnapshot
            Labels
            MetricSnapshot
            Quantile
            Quantiles
            SummarySnapshot$SummaryDataPointSnapshot]))

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

;; ## Histogram and Summary

(defn- get-latest-snapshot [{reg-labels :labels} instance labels]
  (let [snapshot   ^MetricSnapshot (.collect ^MetricWithFixedMetadata instance)
        reg-labels ^"[Ljava.lang.String;" (into-array String reg-labels)
        labels-obj ^Labels (Labels/of reg-labels (collector/ordered-labels reg-labels labels))]
    (loop [datapoints (.getDataPoints snapshot)]
      (when-let [curr-datapoint ^DataPointSnapshot (first datapoints)]
        (if (= (.getLabels curr-datapoint) labels-obj)
          curr-datapoint
          (recur (rest datapoints)))))))

(defn- buckets->vec
  [^HistogramSnapshot$HistogramDataPointSnapshot snapshot]
  (let [buckets     ^ClassicHistogramBuckets (.getClassicBuckets snapshot)
        bucket-vals (->> buckets (.iterator) (iterator-seq)
                         (map #(.getCount ^ClassicHistogramBucket %)))]
    (loop [bs   bucket-vals
           acc 0.0
           bf  []]
      (if-let [b (first bs)]
        (let [nb (+ b acc)]
          (recur (rest bs)
                 nb
                 (conj bf nb)))
        bf))))

(defn- quantiles->map
  [^SummarySnapshot$SummaryDataPointSnapshot snapshot]
  (let [quantiles ^Quantiles (.getQuantiles snapshot)]
    (->> quantiles (.iterator) (iterator-seq)
         (map (fn [^Quantile q] (MapEntry. (.getQuantile q) (.getValue q))))
         (into {}))))

(defn- read-distribution-snapshot-value [type ^DistributionDataPointSnapshot snapshot]
  (cond-> {:count (double (.getCount snapshot))
           :sum   (.getSum snapshot)}
          (= type :histogram) (assoc :buckets (buckets->vec snapshot))
          (= type :summary) (assoc :quantiles (quantiles->map snapshot))))

(extend-type LabeledDistributionCollector
  ReadableCollector
  (read-value [{:keys [collector instance labels]}]
    (when-let [snapshot (get-latest-snapshot collector instance labels)]
      (read-distribution-snapshot-value (:type collector) snapshot)))

  ObservableCollector
  (observe [this amount]
    (.observe ^DistributionDataPoint (.-datapoint this) (double amount)))

  TimeableCollector
  (start-timer [this]
    (start-timer* (.-datapoint this))))

(extend-type LabeledCallbackCollector
  ReadableCollector
  (read-value [{:keys [collector instance labels]}]
   (when-let [snapshot (get-latest-snapshot collector instance labels)]
     (case (:type collector)
       :counter (.getValue ^CounterSnapshot$CounterDataPointSnapshot snapshot)
       :gauge (.getValue ^GaugeSnapshot$GaugeDataPointSnapshot snapshot)
       :summary (read-distribution-snapshot-value :summary snapshot)))))