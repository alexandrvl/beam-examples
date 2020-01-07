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
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.WindowFn
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn
import org.joda.time.Duration
import org.joda.time.Instant

object CtrWindowExample {

  val WindowDurationConf = "windowDuration"
  val ImpressionsSubscriptionConf = "impressionSubscription"
  val ClicksSubscriptionConf = "clickSubscription"
  val CtrsTopicConf = "ctrTopics"

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.optionsAs[StreamingOptions].setStreaming(true)

    val windowDuration = args.int(WindowDurationConf)

    val impressionsSubscription = args.required(ImpressionsSubscriptionConf)
    val clicksSubscription = args.required(ClicksSubscriptionConf)
    val ctrsTopic = args.required(CtrsTopicConf)

    val impressions = sc.pubsubSubscription[Impression](impressionsSubscription)
      .withWindowFn(CtrWindowFn(Duration.standardSeconds(windowDuration)))
      .keyBy(impression => (impression.client, impression.ad))

    val clicks = sc.pubsubSubscription[Click](clicksSubscription)
      .withWindowFn(CtrWindowFn(Duration.standardSeconds(windowDuration)))
      .keyBy(click => (click.client, click.ad))

    val ctrs = impressions
      .leftOuterJoin(clicks)
      .debug()

    //
    //    val aggregatedEvents = eventsByKey
    //      .withSessionWindows(Duration.standardSeconds(windowDuration))
    //      .sumByKey
    //      .values
    //
    //    val filteredAggregatedEvents = aggregatedEvents
    //      .filter { aggregatedEvent => aggregatedEvent.impressions > 0 }
    //
    //    val ctrs = filteredAggregatedEvents
    //      .map { aggregatedEvent => aggregatedEvent.ctr }

    // ctrs.saveAsPubsub(ctrsTopic)

    sc.run()
    ()
  }
}

case class ClientId(id: String) extends AnyVal

case class AdId(id: String) extends AnyVal

sealed trait Event {
  def client(): ClientId

  def ad(): AdId
}

case class Click(client: ClientId, ad: AdId) extends Event

case class Impression(client: ClientId, ad: AdId) extends Event

case class Ctr(client: ClientId, ad: AdId, impressions: Int = 0, clicks: Int = 0) {
  require(impressions >= 0)
  require(clicks >= 0)
}

object Ctr {
  def ctr(): PartialFunction[Ctr, Double] = {
    case ctr: Ctr if ctr.impressions != 0 => ctr.clicks / ctr.impressions
  }
}

class CtrWindow(
    start: Instant,
    end: Instant,
    client: ClientId,
    adId: AdId
) extends IntervalWindow(start, end)

object CtrWindow {
  def forImpression(impression: Impression, timestamp: Instant, gap: Duration): CtrWindow =
    new CtrWindow(timestamp, timestamp.plus(gap), impression.client, impression.ad)

  def forClick(click: Click, timestamp: Instant): CtrWindow =
    new CtrWindow(timestamp, timestamp.plus(1), click.client, click.ad)
}

class CtrWindowFn(gap: Duration) extends WindowFn[Object, CtrWindow] {

  import scala.collection.JavaConverters._

  override def assignWindows(c: WindowFn[Object, CtrWindow]#AssignContext): Collection[CtrWindow] = {
    val event = c.element()
    val timestamp = c.timestamp()
    val ctrWindow = event match {
      case c: Click => CtrWindow.forClick(c, timestamp)
      case i: Impression => CtrWindow.forImpression(i, timestamp, gap)
    }
    Seq(ctrWindow).asJavaCollection
  }

  override def mergeWindows(c: WindowFn[Object, CtrWindow]#MergeContext): Unit = {
    // TODO
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