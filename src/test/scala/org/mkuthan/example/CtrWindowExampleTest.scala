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
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.{Duration => JDuration}
import org.joda.time.{Instant => JInstant}

class CtrWindowExampleTest extends PipelineSpec {

  import CtrWindowExample._

  private val baseTime = new JInstant(0)

  private val eventsSubscription = "events-subscription"
  private val ctrsTopic = "ctrs-topic"

  private val anyEmissionId = EmissionId("emission id")
  private val anyImpression = Event.impression(anyEmissionId)
  private val anyClick = Event.click(anyEmissionId)
  private val anyCtr = Event(anyEmissionId)

  "CTR" should "be calculated from one impression and click in the same window" in {

    val window = new IntervalWindow(at(1 second), at(20 seconds))

    JobTest[CtrWindowExample.type]
      .args(
        s"--$EventsSubscriptionConf=$eventsSubscription",
        s"--$CtrsTopicConf=$ctrsTopic")
      .inputStream(
        PubsubIO.readCoder[Event](
          eventsSubscription,
          idAttribute = Event.IdAttribute,
          timestampAttribute = Event.TimestampAttribute),
        testStreamOf[Event]
          .advanceWatermarkTo(baseTime)
          .addElements(eventAt(anyImpression, 1 second))
          .addElements(eventAt(anyClick, 10 seconds))
          .advanceWatermarkToInfinity())
      .output(
        // TODO: window & pane assertions
        PubsubIO.readCoder[Event](ctrsTopic))(
        _ should inOnTimePane(window) {
          containInAnyOrder(Seq(
            anyCtr.copy(impressions = 1, clicks = 1)
          ))
        }
      )
      .run()
  }

  private def at(duration: Duration): JInstant =
    baseTime.plus(JDuration.millis(duration.toMillis))

  private def eventAt(e: Event, duration: Duration): TimestampedValue[Event] =
    TimestampedValue.of(e, at(duration))

}
