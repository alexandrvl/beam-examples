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

import scala.concurrent.duration._

import com.spotify.scio.io.PubsubIO
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.testStreamOf
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.joda.time.{Duration => JDuration}
import org.joda.time.{Instant => JInstant}

class CtrWindowExampleTest extends PipelineSpec {

  import CtrWindowExample._

  private val baseTime = new JInstant(0)
  private val windowDuration = 60
  private val window = new IntervalWindow(baseTime, JDuration.standardSeconds(windowDuration + 30))

  private val impressionsSubscription = "impressions-subscription"
  private val clicksSubscription = "clicks-subscription"
  private val ctrsTopic = "ctrs-topic"

  private val anyClient = ClientId("any client id")
  private val anyAd = AdId("any ad id")
  private val anyImpression = Impression(anyClient, anyAd)
  private val anyClick = Click(anyClient, anyAd)
  private val anyCtr = Ctr(anyClient, anyAd)

  "CTR" should "be calculated from clicks and impressions" in {

    val adOneId = AdId("ad id 1")
    val adTwoId = AdId("ad id 2")

    JobTest[CtrWindowExample.type]
      .args(
        s"--$WindowDurationConf=$windowDuration",
        s"--$ImpressionsSubscriptionConf=$impressionsSubscription",
        s"--$ClicksSubscriptionConf=$clicksSubscription",
        s"--$CtrsTopicConf=$ctrsTopic")
      .inputStream(
        PubsubIO[Impression](impressionsSubscription), testStreamOf[Impression]
          .advanceWatermarkTo(baseTime)
          .addElements(anyImpression.copy(ad = adOneId))
          .advanceWatermarkTo(baseTimePlus(10 seconds))
          .addElements(anyImpression.copy(ad = adTwoId))
          .advanceWatermarkToInfinity())
      .inputStream(
        PubsubIO[Click](clicksSubscription), testStreamOf[Click]
          .advanceWatermarkTo(baseTimePlus(30 seconds))
          .addElements(anyClick.copy(ad = adOneId))
          .advanceWatermarkToInfinity())
      .output(
        PubsubIO[Ctr](ctrsTopic))(
        _ should inWindow(window) {
          containInAnyOrder(Seq(
            anyCtr.copy(ad = adOneId, impressions = 1, clicks = 1)
          ))
        })
      .run()
  }

  private def baseTimePlus(duration: Duration): JInstant =
    baseTime.plus(JDuration.millis(duration.toMillis))

}
