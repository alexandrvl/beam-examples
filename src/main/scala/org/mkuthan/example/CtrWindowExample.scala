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

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.CoderMaterializer
import com.twitter.algebird.Semigroup
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.WindowFn
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn
import org.joda.time.Duration
import org.joda.time.Instant

object CtrWindowExample {

  val WindowDurationConf = "windowDuration"
  val EventsSubscriptionConf = "eventSubscription"
  val CtrsTopicConf = "ctrTopics"

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.optionsAs[StreamingOptions].setStreaming(true)

    val windowDuration = args.int(WindowDurationConf)

    val eventsSubscription = args.required(EventsSubscriptionConf)
    val ctrsTopic = args.required(CtrsTopicConf)

    val events = sc.pubsubSubscription[Event](eventsSubscription)
      .withWindowFn(CtrWindowFn(Duration.standardSeconds(windowDuration)))

    val eventsByKey = events.keyBy(event => event.key)
    val ctrs = eventsByKey
      .sumByKey(new EventSemigroup())
      .values

    ctrs.saveAsPubsub(ctrsTopic)

    sc.run()
    ()
  }
}

case class ClientId(id: String) extends AnyVal

case class AdId(id: String) extends AnyVal

case class Event(client: ClientId, ad: AdId, impressions: Int = 0, clicks: Int = 0) {
  require(impressions >= 0, s"No of impressions must be non-negative but was $impressions")
  require(clicks >= 0, s"No of clicks must be non-negative but was $clicks")

  lazy val key: (ClientId, AdId) = (client, ad)
  lazy val isImpression: Boolean = clicks == 0 && impressions == 1
  lazy val isClick: Boolean = clicks == 1 && impressions == 0
}

object Event {
  def impression(client: ClientId, ad: AdId): Event = new Event(client, ad, impressions = 1)

  def click(client: ClientId, ad: AdId): Event = new Event(client, ad, clicks = 1)

  def ctr(): PartialFunction[Event, Double] = {
    case e: Event if e.impressions != 0 => math.max(e.clicks, 1) / math.max(e.impressions, 1)
  }
}

class EventSemigroup extends Semigroup[Event] {
  override def plus(e1: Event, e2: Event): Event = {
    require(e1.key == e2.key)

    new Event(e1.client, e1.ad, e1.impressions + e2.impressions, e1.clicks + e2.clicks)
  }
}

class CtrWindow(
    start: Instant,
    end: Instant,
    client: ClientId,
    ad: AdId,
    private val isClick: Boolean
) extends IntervalWindow(start, end) {
  lazy val key: (ClientId, AdId) = (client, ad)

  def merge(other: CtrWindow): CtrWindow = {
    require(key == other.key)

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
      client,
      ad,
      isClick || other.isClick
    )
  }
}

object CtrWindow {
  def forImpression(e: Event, timestamp: Instant, gap: Duration): CtrWindow =
    new CtrWindow(timestamp, timestamp.plus(gap), e.client, e.ad, false)

  def forClick(e: Event, timestamp: Instant): CtrWindow =
    new CtrWindow(timestamp, timestamp.plus(1), e.client, e.ad, true)
}

class CtrWindowFn(gap: Duration) extends WindowFn[AnyRef, CtrWindow] {

  import scala.collection.JavaConverters._

  override def assignWindows(c: WindowFn[AnyRef, CtrWindow]#AssignContext): Collection[CtrWindow] = {
    val event = c.element()
    val timestamp = c.timestamp()
    val ctrWindow = event match {
      case e: Event if e.isImpression => CtrWindow.forImpression(e, timestamp, gap)
      case e: Event if e.isClick => CtrWindow.forClick(e, timestamp)
    }
    Seq(ctrWindow).asJavaCollection
  }

  override def mergeWindows(c: WindowFn[AnyRef, CtrWindow]#MergeContext): Unit = {
    val windows = c.windows().asScala
    val windowsByKey = windows.groupBy(w => w.key)
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
  def apply(gap: Duration): CtrWindowFn = new CtrWindowFn(gap)
}
