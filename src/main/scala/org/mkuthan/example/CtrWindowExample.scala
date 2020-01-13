// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.mkuthan.example

import java.util.Collection
import java.util.Objects

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.CoderMaterializer
import com.spotify.scio.values.WindowOptions
import com.twitter.algebird.Semigroup
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior
import org.apache.beam.sdk.transforms.windowing.WindowFn
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.joda.time.Instant

object CtrWindowExample {

  val ImpressionToClickWindowDurationConf = "impressionToClickWindowDuration"
  val ClickToImpressionWindowDurationConf = "clickToImpressionWindowDuration"
  val AllowedLatenessConf = "allowedLateness"

  val EventsSubscriptionConf = "eventSubscription"
  val CtrsTopicConf = "ctrTopics"

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.optionsAs[StreamingOptions].setStreaming(true)

    val impressionToClickWindowDuration = args.int(ImpressionToClickWindowDurationConf, 300)
    val clickToImpressionWindowDuration = args.int(ClickToImpressionWindowDurationConf, 10)
    val allowedLateness = args.int(AllowedLatenessConf, 3600)

    val eventsSubscription = args.required(EventsSubscriptionConf)
    val ctrsTopic = args.required(CtrsTopicConf)

    val events = sc.pubsubSubscription[Event](
      eventsSubscription,
      idAttribute = Event.IdAttribute, timestampAttribute = Event.TimestampAttribute)

    val windowOptions = WindowOptions(
      trigger = AfterWatermark
        .pastEndOfWindow()
        .withEarlyFirings(
          AfterProcessingTime
            .pastFirstElementInPane()
            .plusDelayOf(Duration.standardSeconds(30))
        )
        .withLateFirings(
          AfterProcessingTime
            .pastFirstElementInPane()
            .plusDelayOf(Duration.standardSeconds(600))
        ),
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES,
      allowedLateness = Duration.standardSeconds(allowedLateness),
      closingBehavior = ClosingBehavior.FIRE_ALWAYS,
      timestampCombiner = TimestampCombiner.LATEST
    )

    val eventsById = events
      .withWindowFn(
        CtrWindowFn(
          Duration.standardSeconds(impressionToClickWindowDuration),
          Duration.standardSeconds(clickToImpressionWindowDuration)
        ),
        options = windowOptions
      )
      .keyBy(event => event.emissionId)

    val ctrs = eventsById
      .sumByKey(new EventSemigroup())
      .values

    ctrs.withWindow.debug()
    ctrs.withPaneInfo.debug()

    ctrs.saveAsPubsub(ctrsTopic)

    sc.run()
    ()
  }
}

case class EmissionId(id: String) extends AnyVal

case class Event(emissionId: EmissionId, impressions: Int = 0, clicks: Int = 0) {
  require(impressions >= 0, s"No of impressions must be non-negative but was $impressions")
  require(clicks >= 0, s"No of clicks must be non-negative but was $clicks")

  lazy val isImpression: Boolean = clicks == 0 && impressions == 1
  lazy val isClick: Boolean = clicks == 1 && impressions == 0
}

object Event {
  val IdAttribute = "id"
  val TimestampAttribute = "ts"

  def impression(emissionId: EmissionId): Event = new Event(emissionId, impressions = 1)

  def click(emissionId: EmissionId): Event = new Event(emissionId, clicks = 1)

  def ctr(): PartialFunction[Event, Double] = {
    case e: Event if e.impressions != 0 => math.max(e.clicks, 1) / math.max(e.impressions, 1)
  }
}

class EventSemigroup extends Semigroup[Event] {
  override def plus(e1: Event, e2: Event): Event = {
    require(e1.emissionId == e2.emissionId)

    new Event(
      e1.emissionId,
      e1.impressions + e2.impressions,
      e1.clicks + e2.clicks)
  }
}

// TODO: change into case class and get rid of equals/hashCode/toString
class CtrWindow(
    val start: Instant,
    val end: Instant,
    val emissionId: EmissionId,
    val isClick: Boolean
) extends BoundedWindow {

  override def maxTimestamp(): Instant = end

  override def equals(obj: Any): Boolean =
    obj match {
      case other: CtrWindow => start == other.start &&
        end == other.end &&
        emissionId == other.emissionId &&
        isClick == other.isClick
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(start, end, emissionId.id, Boolean.box(isClick))

  override def toString: String =
    s"CtrWindow{start=$start, end=$end, emissionId='${emissionId.id}', isClick='$isClick'}"

  def merge(other: CtrWindow): CtrWindow = {
    require(emissionId == other.emissionId)

    val thisStart = start.getMillis
    val thisEnd = end.getMillis

    val otherStart = other.start.getMillis
    val otherEnd = other.end.getMillis

    val newStart = math.min(thisStart, otherStart)
    val newEnd = if (isClick && other.isClick) {
      math.min(thisEnd, otherEnd)
    } else if (isClick) {
      thisEnd
    } else if (other.isClick) {
      otherEnd
    } else { // impressions
      math.max(thisEnd, otherEnd)
    }

    new CtrWindow(
      Instant.ofEpochMilli(newStart),
      Instant.ofEpochMilli(newEnd),
      emissionId,
      isClick || other.isClick
    )
  }
}

object CtrWindow {
  def forImpression(e: Event, timestamp: Instant, impressionToClickWindowDuration: Duration): CtrWindow =
    new CtrWindow(timestamp, timestamp.plus(impressionToClickWindowDuration), e.emissionId, false)

  def forClick(e: Event, timestamp: Instant, clickToImpressionWindowDuration: Duration): CtrWindow =
    new CtrWindow(timestamp, timestamp.plus(clickToImpressionWindowDuration), e.emissionId, true)
}

// TODO: change Window[AnyRef, CtrWindow] into Window[Event, CtrWindow]
class CtrWindowFn(
    impressionToClickWindowDuration: Duration,
    clickToImpressionWindowDuration: Duration
) extends WindowFn[AnyRef, CtrWindow] {

  import scala.collection.JavaConverters._

  override def assignWindows(c: WindowFn[AnyRef, CtrWindow]#AssignContext): Collection[CtrWindow] = {
    val event = c.element()
    val timestamp = c.timestamp()
    val ctrWindow = event match {
      case e: Event if e.isImpression => CtrWindow.forImpression(e, timestamp, impressionToClickWindowDuration)
      case e: Event if e.isClick => CtrWindow.forClick(e, timestamp, clickToImpressionWindowDuration)
    }
    Seq(ctrWindow).asJavaCollection
  }

  override def mergeWindows(c: WindowFn[AnyRef, CtrWindow]#MergeContext): Unit = {
    val windows = c.windows().asScala
    val windowsByKey = windows.groupBy(w => w.emissionId)
    val mergedWindows = windowsByKey
      .mapValues(ws =>
        ws.reduce { (w1, w2) =>
          w1.merge(w2)
        }
      )

    windowsByKey.foreach { case (k, ws) =>
      c.merge(ws.asJavaCollection, mergedWindows(k))
    }
  }

  override def isCompatible(other: WindowFn[_, _]): Boolean =
    other.isInstanceOf[CtrWindowFn]

  override def windowCoder(): Coder[CtrWindow] =
    CoderMaterializer.beamWithDefault(com.spotify.scio.coders.Coder[CtrWindow])

  override def getDefaultWindowMappingFn: WindowMappingFn[CtrWindow] =
    throw new UnsupportedOperationException()
}

object CtrWindowFn {
  def apply(
      impressionToClickWindowDuration: Duration,
      clickToImpressionWindowDuration: Duration
  ): CtrWindowFn = new CtrWindowFn(impressionToClickWindowDuration, clickToImpressionWindowDuration)
}
