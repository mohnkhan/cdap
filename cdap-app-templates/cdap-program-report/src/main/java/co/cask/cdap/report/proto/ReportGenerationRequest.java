/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.proto;

import co.cask.cdap.report.util.ReportField;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Represents a request to generate a program run report in an HTTP request.
 */
public class ReportGenerationRequest {
  private final Long start;
  private final Long end;
  private final List<String> fields;
  @Nullable
  private final List<Sort> sort;
  @Nullable
  private final List<Filter> filters;

  public ReportGenerationRequest(Long start, Long end, List<String> fields, @Nullable List<Sort> sort,
                                 @Nullable List<Filter> filters) {
    this.start = start;
    this.end = end;
    this.fields = fields;
    this.sort = sort;
    this.filters = filters;
    this.validate();
  }

  /**
   * @return the start of the time range within which the report is generated. All program runs in the report must be
   *         still running at {@code start}, or with end time not earlier than {@code start}.
   */
  public Long getStart() {
    return start;
  }

  /**
   * @return the end of the time range within which the report is generated. All program runs in the report must be
   *         still start before {@code end}.
   */
  public Long getEnd() {
    return end;
  }

  /**
   * @return names of the fields to be included in the final report. Must be valid fields from {@link ReportField}.
   */
  public List<String> getFields() {
    return fields;
  }

  /**
   * @return the field to sort the report by. Currently only support a single field.
   */
  @Nullable
  public List<Sort> getSort() {
    return sort;
  }

  /**
   * @return the filters that must be satisfied for every record in the report.
   */
  @Nullable
  public List<Filter> getFilters() {
    return filters;
  }

  /**
   * Validates this {@link ReportGenerationRequest}
   *
   * @throws IllegalArgumentException if this request is not valid.
   */
  public void validate() {
    List<String> errors = new ArrayList<>();
    if (start == null) {
      errors.add("'start' must be specified.");
    }
    if (end == null) {
      errors.add("'end' must be specified.");
    }
    if (fields == null || fields.isEmpty()) {
      errors.add("'fields' must be specified.");
    } else {
      errors.addAll(fields.stream().filter(field -> !ReportField.isValidField(field))
                      .map(field -> String.format("Invalid field name '%s' in fields.", field))
                      .collect(Collectors.toList()));
    }
    // No need to check whether filter field name and type are valid since this is done during JSON deserialization
    if (filters != null) {
      Set<String> uniqueFilterFields = new HashSet<>();
      errors.addAll(filters.stream().filter(filter -> !uniqueFilterFields.add(filter.getFieldName()))
                      .map(filter -> String.format("Field '%s' is duplicated in filters.", filter.getFieldName()))
                      .collect(Collectors.toList()));
    }
    if (sort != null) {
      if (sort.size() > 1) {
        errors.add("Currently only one field is supported in sort.");
      }
      for (Sort sortField : sort) {
        ReportField sortFieldType = ReportField.valueOfFieldName(sortField.getFieldName());
        if (sortFieldType == null) {
          errors.add(String.format("Invalid field name '%s' in sort.", sortField.getFieldName()));
        }
        if (!sortFieldType.isSortable()) {
          errors.add(String.format("Field '%s' in sort is not sortable.", sortField.getFieldName()));
        }
      }
    }
    if (errors.size() > 0) {
      throw new IllegalArgumentException("Invalid report generation request: " + String.join(",", errors));
    }
  }

  /**
   * Represents a flied in the report.
   */
  public static class Field {
    private final String fieldName;

    public Field(String fieldName) {
      this.fieldName = fieldName;
    }

    public String getFieldName() {
      return fieldName;
    }
  }

  /**
   * A class represents the field to sort the report by and the order of sorting by this field.
   */
  public static class Sort extends Field {
    private final Order order;

    public Sort(String fieldName, Order order) {
      super(fieldName);
      this.order = order;
    }

    /**
     * @return the sorting order with this field
     */
    public Order getOrder() {
      return order;
    }

    @Override
    public String toString() {
      return "Sort{" +
        "fieldName=" + getFieldName() +
        ", order=" + order +
        '}';
    }
  }

  /**
   * Represents a filter that can be applied to a field and determine whether the field's value is allowed to
   * be included in the report.
   *
   * @param <T> type of the values
   */
  public abstract static class Filter<T> extends Field {
    public Filter(String fieldName) {
      super(fieldName);
    }

    /**
     * Checks whether the given value of the field is allowed.
     * @param value value of the field
     * @return {@code true} if the value is allowed, {@code false} otherwise.
     */
    public abstract boolean apply(T value);
  }

  /**
   * Represents a filter that checks whether a given value of a field is one of the allowed values and is not one of
   * the forbidden values.
   *
   * @param <T> type of the values
   */
  public static class ValueFilter<T> extends Filter<T> {
    @Nullable
    private final List<T> whitelist;
    @Nullable
    private final List<T> blacklist;

    public ValueFilter(String fieldName, @Nullable List<T> whitelist, @Nullable List<T> blacklist) {
      super(fieldName);
      this.whitelist = whitelist;
      this.blacklist = blacklist;
    }

    /**
     * @return the allowed values of this field, or {@code null} if there's no such limit
     */
    @Nullable
    public List<T> getWhitelist() {
      return whitelist;
    }

    /**
     * @return the disallowed values of this field, or {@code null} if there's no such limit
     */
    @Nullable
    public List<T> getBlacklist() {
      return blacklist;
    }

    @Override
    public boolean apply(T value) {
      return (whitelist == null || whitelist.isEmpty() || whitelist.contains(value))
        && (blacklist == null || blacklist.isEmpty() || !blacklist.contains(value));
    }

    @Override
    public String toString() {
      return "ValueFilter{" +
        "fieldName=" + getFieldName() +
        ", whitelist=" + whitelist +
        ", blacklist=" + blacklist +
        '}';
    }
  }

  /**
   * Represents a filter that checks whether a given value of a field is within the allowed range.
   *
   * @param <T> type of the values
   */
  public static class RangeFilter<T extends Comparable<T>> extends Filter<T> {
    private final Range<T> range;

    public RangeFilter(String fieldName, Range<T> range) {
      super(fieldName);
      this.range = range;
    }

    /**
     * @return the allowed range of values of this field
     */
    public Range getRange() {
      return range;
    }

    @Override
    public boolean apply(T value) {
      return (range.getMin() == null || range.getMin().compareTo(value) <= 0)
        && (range.getMax() == null || range.getMax().compareTo(value) > 0);
    }
    @Override
    public String toString() {
      return "RangeFilter{" +
        "fieldName=" + getFieldName() +
        ", range=" + range +
        '}';
    }
  }

  /**
   * Range of allowed values of a field represented as [min, max) where min is the inclusive minimum value
   * and max is the exclusive maximum value.
   *
   * @param <T> the value type of the field
   */
  public static class Range<T> {
    @Nullable
    private final T min;
    @Nullable
    private final T max;

    public Range(T min, T max) {
      this.min = min;
      this.max = max;
    }

    /**
     * @return the inclusive minimum value of this range
     */
    @Nullable
    public T getMin() {
      return min;
    }

    /**
     * @return the exclusive maximum value of this range
     */
    @Nullable
    public T getMax() {
      return max;
    }

    @Override
    public String toString() {
      return "Range{" +
        "min=" + min +
        ", max=" + max +
        '}';
    }
  }

  /**
   * The order to sort a field by.
   */
  public enum Order {
    ASCENDING,
    DESCENDING
  }
}
