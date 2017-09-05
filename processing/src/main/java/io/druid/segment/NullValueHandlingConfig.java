package io.druid.segment;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NullValueHandlingConfig
{
  private static boolean DEFAULT_USE_DEFAULT_VALUES_FOR_NULL = true;

  @JsonProperty("useDefaultValueForNull")
  private Boolean useDefaultValuesForNull = DEFAULT_USE_DEFAULT_VALUES_FOR_NULL;

  @JsonCreator
  public NullValueHandlingConfig(@JsonProperty("useDefaultValueForNull") Boolean useDefaultValuesForNull)
  {
    this.useDefaultValuesForNull = useDefaultValuesForNull == null
                                   ? DEFAULT_USE_DEFAULT_VALUES_FOR_NULL
                                   : useDefaultValuesForNull;
  }


  public boolean isUseDefaultValuesForNull()
  {
    return useDefaultValuesForNull;
  }
}
