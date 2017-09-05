package io.druid.guice;

import com.google.inject.Injector;
import io.druid.segment.NullHandlingHelper;
import org.junit.Assert;
import org.junit.Test;

public class NullHandlingHelperInjectionTest
{
  @Test
  public void testNullHandlingHelperUseDefaultValues(){
    System.setProperty("druid.null.handling.useDefaultValueForNull", "true");
    Injector injector = GuiceInjectors.makeStartupInjector();
    Assert.assertEquals(true, NullHandlingHelper.useDefaultValuesForNull());
  }

  @Test
  public void testNullHandlingHelperNoDefaultValues(){
    System.setProperty("druid.null.handling.useDefaultValueForNull", "false");
    Injector injector = GuiceInjectors.makeStartupInjector();
    Assert.assertEquals(false, NullHandlingHelper.useDefaultValuesForNull());
  }

  @Test
  public void testStaticInjectionNullHandlingHelperDefault(){
    Injector injector = GuiceInjectors.makeStartupInjector();
    Assert.assertEquals(true, NullHandlingHelper.useDefaultValuesForNull());
  }
}
