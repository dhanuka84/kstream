package io.confluent.developer;


import org.junit.Test;

import java.util.HashMap;

import io.confluent.developer.avro.UserEvent;
import io.confluent.developer.avro.UserRollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class LoginAggregatorTest {

  @Test
  public void shouldAggregateValues() {
    final LoginAggregator loginAggregator = new LoginAggregator();
    final UserRollup loginRollup = new UserRollup();
    loginRollup.setEventByUser(new HashMap<>());

    final String appOne = "app-one";
    final String appTwo = "app-two";

    final String user1 = "user1";
    final String user2 = "user2";

    loginAggregator.apply(user1, login(appOne, user1), loginRollup);
    loginAggregator.apply(user2, login(appOne, user2), loginRollup);
    

    assertThat(loginRollup.getEventByUser().get(user1).get(appOne), is(1L));
    assertThat(loginRollup.getEventByUser().get(user2).get(appOne), is(1L));
    

    loginAggregator.apply(user1, login(appTwo, user1), loginRollup);
    loginAggregator.apply(user2, login(appTwo, user2), loginRollup);

    assertThat(loginRollup.getEventByUser().get(user1).get(appTwo), is(1L));
    assertThat(loginRollup.getEventByUser().get(user2).get(appTwo), is(1L));
    

    loginAggregator.apply(user1, login(appOne, user1), loginRollup);
    loginAggregator.apply(user2, login(appOne, user2), loginRollup);
    

    loginAggregator.apply(user1, login(appTwo, user1), loginRollup);
    loginAggregator.apply(user2, login(appTwo, user2), loginRollup);
    

    assertThat(loginRollup.getEventByUser().get(user1).get(appOne), is(2L));
    assertThat(loginRollup.getEventByUser().get(user2).get(appTwo), is(2L));
   

  }

  private UserEvent login(String appId, String userId) {
       return new UserEvent(appId, userId, System.currentTimeMillis());
  }
}
