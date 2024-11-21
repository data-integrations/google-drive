/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.google.common;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.ExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({GoogleJsonResponseException.class})
public class APIRequestRetryerTest {
  private static final int UNPROCESSED_CODE = 504;
  private static final int RETRY_NUMBER = 10;

  @Test
  public void testRetryCount() throws ExecutionException {
    GoogleJsonResponseException exception = PowerMockito.mock(GoogleJsonResponseException.class);
    PowerMockito.when(exception.getStatusCode()).thenReturn(APIRequestRetryer.TOO_MANY_REQUESTS_CODE);
    PowerMockito.when(exception.getStatusMessage()).thenReturn(APIRequestRetryer.TOO_MANY_REQUESTS_MESSAGE);


    TestAPIClass testAPIClass = new TestAPIClass(exception);

    Retryer<Void> testRetryer = APIRequestRetryer.getRetryer("Test request");
    try {
      testRetryer.call(() -> testAPIClass.call());
      Assert.fail("Test api call should be failed.");
    } catch (RetryException e) {
    }
    Assert.assertEquals(RETRY_NUMBER, testAPIClass.counter);
  }

  @Test
  public void testNotCatchedException() {
    GoogleJsonResponseException exception = PowerMockito.mock(GoogleJsonResponseException.class);
    GoogleJsonError googleJsonError = new GoogleJsonError();
    googleJsonError.setCode(UNPROCESSED_CODE);
    PowerMockito.when(exception.getDetails()).thenReturn(googleJsonError);


    TestAPIClass testAPIClass = new TestAPIClass(exception);

    Retryer<Void> testRetryer = APIRequestRetryer.getRetryer("Test request");
    try {
      testRetryer.call(() -> testAPIClass.call());
      Assert.fail("Test api call should be failed.");
    } catch (ExecutionException e) {
      // required exception
      Assert.assertEquals("googleJsonResponseException", e.getMessage());
    } catch (RetryException e) {
      Assert.fail("Test api call should be failed after on execution.");
    }
    Assert.assertEquals(1, testAPIClass.counter);
  }

  private static class TestAPIClass {
    final Exception exception;
    int counter = 0;

    TestAPIClass(Exception exception) {
      this.exception = exception;
    }

    public Void call() throws Exception {
      counter++;
      throw exception;
    }
  }
}
