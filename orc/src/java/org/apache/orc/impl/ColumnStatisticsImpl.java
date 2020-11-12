/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.orc.*;

public class ColumnStatisticsImpl implements ColumnStatistics {

  private static final class BooleanStatisticsImpl extends ColumnStatisticsImpl
      implements BooleanColumnStatistics {
    private long trueCount = 0;

    BooleanStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.BucketStatistics bkt = stats.getBucketStatistics();
      trueCount = bkt.getCount(0);
    }

    BooleanStatisticsImpl() {
    }

    @Override
    public void reset() {
      super.reset();
      trueCount = 0;
    }

    @Override
    public void updateBoolean(boolean value, int repetitions) {
      if (value) {
        trueCount += repetitions;
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof BooleanStatisticsImpl) {
        BooleanStatisticsImpl bkt = (BooleanStatisticsImpl) other;
        trueCount += bkt.trueCount;
      } else {
        if (isStatsExists() && trueCount != 0) {
          throw new IllegalArgumentException("Incompatible merging of boolean column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.BucketStatistics.Builder bucket =
        OrcProto.BucketStatistics.newBuilder();
      bucket.addCount(trueCount);
      builder.setBucketStatistics(bucket);
      return builder;
    }

    @Override
    public long getFalseCount() {
      return getNumberOfValues() - trueCount;
    }

    @Override
    public long getTrueCount() {
      return trueCount;
    }

    @Override
    public String toString() {
      return super.toString() + " true: " + trueCount;
    }
  }

  private static final class IntegerStatisticsImpl extends ColumnStatisticsImpl
      implements IntegerColumnStatistics {

    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;
    private long sum = 0;
    private boolean hasMinimum = false;
    private boolean overflow = false;

    IntegerStatisticsImpl() {
    }

    IntegerStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.IntegerStatistics intStat = stats.getIntStatistics();
      if (intStat.hasMinimum()) {
        hasMinimum = true;
        minimum = intStat.getMinimum();
      }
      if (intStat.hasMaximum()) {
        maximum = intStat.getMaximum();
      }
      if (intStat.hasSum()) {
        sum = intStat.getSum();
      } else {
        overflow = true;
      }
    }

    @Override
    public void reset() {
      super.reset();
      hasMinimum = false;
      minimum = Long.MAX_VALUE;
      maximum = Long.MIN_VALUE;
      sum = 0;
      overflow = false;
    }

    @Override
    public void updateInteger(long value, int repetitions) {
      if (!hasMinimum) {
        hasMinimum = true;
        minimum = value;
        maximum = value;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      if (!overflow) {
        boolean wasPositive = sum >= 0;
        sum += value * repetitions;
        if ((value >= 0) == wasPositive) {
          overflow = (sum >= 0) != wasPositive;
        }
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof IntegerStatisticsImpl) {
        IntegerStatisticsImpl otherInt = (IntegerStatisticsImpl) other;
        if (!hasMinimum) {
          hasMinimum = otherInt.hasMinimum;
          minimum = otherInt.minimum;
          maximum = otherInt.maximum;
        } else if (otherInt.hasMinimum) {
          if (otherInt.minimum < minimum) {
            minimum = otherInt.minimum;
          }
          if (otherInt.maximum > maximum) {
            maximum = otherInt.maximum;
          }
        }

        overflow |= otherInt.overflow;
        if (!overflow) {
          boolean wasPositive = sum >= 0;
          sum += otherInt.sum;
          if ((otherInt.sum >= 0) == wasPositive) {
            overflow = (sum >= 0) != wasPositive;
          }
        }
      } else {
        if (isStatsExists() && hasMinimum) {
          throw new IllegalArgumentException("Incompatible merging of integer column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.IntegerStatistics.Builder intb =
        OrcProto.IntegerStatistics.newBuilder();
      if (hasMinimum) {
        intb.setMinimum(minimum);
        intb.setMaximum(maximum);
      }
      if (!overflow) {
        intb.setSum(sum);
      }
      builder.setIntStatistics(intb);
      return builder;
    }

    @Override
    public long getMinimum() {
      return minimum;
    }

    @Override
    public long getMaximum() {
      return maximum;
    }

    @Override
    public boolean isSumDefined() {
      return !overflow;
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (hasMinimum) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
      }
      if (!overflow) {
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }
  }

  private static final class DoubleStatisticsImpl extends ColumnStatisticsImpl
       implements DoubleColumnStatistics {
    private boolean hasMinimum = false;
    private double minimum = Double.MAX_VALUE;
    private double maximum = Double.MIN_VALUE;
    private double sum = 0;

    DoubleStatisticsImpl() {
    }

    DoubleStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DoubleStatistics dbl = stats.getDoubleStatistics();
      if (dbl.hasMinimum()) {
        hasMinimum = true;
        minimum = dbl.getMinimum();
      }
      if (dbl.hasMaximum()) {
        maximum = dbl.getMaximum();
      }
      if (dbl.hasSum()) {
        sum = dbl.getSum();
      }
    }

    @Override
    public void reset() {
      super.reset();
      hasMinimum = false;
      minimum = Double.MAX_VALUE;
      maximum = Double.MIN_VALUE;
      sum = 0;
    }

    @Override
    public void updateDouble(double value) {
      if (!hasMinimum) {
        hasMinimum = true;
        minimum = value;
        maximum = value;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      sum += value;
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof DoubleStatisticsImpl) {
        DoubleStatisticsImpl dbl = (DoubleStatisticsImpl) other;
        if (!hasMinimum) {
          hasMinimum = dbl.hasMinimum;
          minimum = dbl.minimum;
          maximum = dbl.maximum;
        } else if (dbl.hasMinimum) {
          if (dbl.minimum < minimum) {
            minimum = dbl.minimum;
          }
          if (dbl.maximum > maximum) {
            maximum = dbl.maximum;
          }
        }
        sum += dbl.sum;
      } else {
        if (isStatsExists() && hasMinimum) {
          throw new IllegalArgumentException("Incompatible merging of double column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.DoubleStatistics.Builder dbl =
        OrcProto.DoubleStatistics.newBuilder();
      if (hasMinimum) {
        dbl.setMinimum(minimum);
        dbl.setMaximum(maximum);
      }
      dbl.setSum(sum);
      builder.setDoubleStatistics(dbl);
      return builder;
    }

    @Override
    public double getMinimum() {
      return minimum;
    }

    @Override
    public double getMaximum() {
      return maximum;
    }

    @Override
    public double getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (hasMinimum) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
      }
      buf.append(" sum: ");
      buf.append(sum);
      return buf.toString();
    }
  }

  protected static final class StringStatisticsImpl extends ColumnStatisticsImpl
      implements StringColumnStatistics {
    public static final int MAX_BYTES_RECORDED = 1024;
    private Text minimum = null;
    private Text maximum = null;
    private long sum = 0;

    private boolean isLowerBoundSet = false;
    private boolean isUpperBoundSet = false;

    StringStatisticsImpl() {
    }

    StringStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.StringStatistics str = stats.getStringStatistics();
      if (str.hasMaximum()) {
        maximum = new Text(str.getMaximum());
      }
      if (str.hasMinimum()) {
        minimum = new Text(str.getMinimum());
      }
      if(str.hasSum()) {
        sum = str.getSum();
      }
    }

    @Override
    public void reset() {
      super.reset();
      minimum = null;
      maximum = null;
      isLowerBoundSet = false;
      isUpperBoundSet = false;
      sum = 0;
    }

    @Override
    public void updateString(Text value) {
      updateString(value.getBytes(), 0, value.getLength(), 1);
    }

    @Override
    public void updateString(byte[] bytes, int offset, int length,
                             int repetitions) {
      if (minimum == null) {
        if(length > MAX_BYTES_RECORDED) {
          minimum = truncateLowerBound(bytes, offset);
          maximum = truncateUpperBound(bytes, offset);
          isLowerBoundSet = true;
          isUpperBoundSet = true;
        } else {
          maximum = minimum = new Text();
          maximum.set(bytes, offset, length);
          isLowerBoundSet = false;
          isUpperBoundSet = false;
        }
      } else if (WritableComparator.compareBytes(minimum.getBytes(), 0,
          minimum.getLength(), bytes, offset, length) > 0) {
        if(length > MAX_BYTES_RECORDED) {
          minimum = truncateLowerBound(bytes, offset);
          isLowerBoundSet = true;
        } else {
          minimum = new Text();
          minimum.set(bytes, offset, length);
          isLowerBoundSet = false;
        }
      } else if (WritableComparator.compareBytes(maximum.getBytes(), 0,
          maximum.getLength(), bytes, offset, length) < 0) {
        if(length > MAX_BYTES_RECORDED) {
          maximum = truncateUpperBound(bytes, offset);
          isUpperBoundSet = true;
        } else {
          maximum = new Text();
          maximum.set(bytes, offset, length);
          isUpperBoundSet = false;
        }
      }
      sum += (long)length * repetitions;
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof StringStatisticsImpl) {
        StringStatisticsImpl str = (StringStatisticsImpl) other;
        if (minimum == null) {
          if (str.minimum != null) {
            maximum = new Text(str.getMaximum());
            minimum = new Text(str.getMinimum());
          }
          /* str.minimum == null when lower bound set */
          else if (str.isLowerBoundSet) {
            minimum = new Text(str.getLowerBound());
            isLowerBoundSet = str.isLowerBoundSet;

            /* check for upper bound before setting max */
            if (str.isUpperBoundSet) {
              maximum = new Text(str.getUpperBound());
              isUpperBoundSet = str.isUpperBoundSet;
            } else {
              maximum = new Text(str.getMaximum());
            }
          }
          else {
            /* both are empty */
            maximum = minimum = null;
          }
        } else if (str.minimum != null) {
          if (minimum.compareTo(str.minimum) > 0) {
            if(str.isLowerBoundSet) {
              minimum = new Text(str.getLowerBound());
              isLowerBoundSet = str.isLowerBoundSet;
            } else {
              minimum = new Text(str.getMinimum());
            }
          }
          if (maximum.compareTo(str.maximum) < 0) {
            if(str.isUpperBoundSet) {
              maximum = new Text(str.getUpperBound());
              isUpperBoundSet = str.isUpperBoundSet;
            }else {
              maximum = new Text(str.getMaximum());
            }
          }
        }
        sum += str.sum;
      } else {
        if (isStatsExists() && minimum != null) {
          throw new IllegalArgumentException("Incompatible merging of string column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.StringStatistics.Builder str =
          OrcProto.StringStatistics.newBuilder();
      if (getNumberOfValues() != 0) {
        str.setMinimum(getMinimum());
        str.setMaximum(getMaximum());
        str.setSum(sum);
      }
      result.setStringStatistics(str);
      return result;
    }

    @Override
    public String getMinimum() {
      /* if we have lower bound set (in case of truncation)
      getMinimum will be null */
      if(isLowerBoundSet) {
        return null;
      } else {
        return minimum == null ? null : minimum.toString();
      }
    }

    @Override
    public String getMaximum() {
      /* if we have upper bound is set (in case of truncation)
      getMaximum will be null */
      if(isUpperBoundSet) {
        return null;
      } else {
        return maximum == null ? null : maximum.toString();
      }
    }

    /**
     * Get the string with
     * length = Min(StringStatisticsImpl.MAX_BYTES_RECORDED, getMinimum())
     *
     * @return lower bound
     */
    @Override
    public String getLowerBound() {
      return minimum == null ? null : minimum.toString();
    }

    /**
     * Get the string with
     * length = Min(StringStatisticsImpl.MAX_BYTES_RECORDED, getMaximum())
     *
     * @return upper bound
     */
    @Override
    public String getUpperBound() {
      return maximum == null ? null : maximum.toString();
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (minimum != null) {
        if (isLowerBoundSet) {
          buf.append(" lower: ");
        } else {
          buf.append(" min: ");
        }
        buf.append(getLowerBound());
        if (isUpperBoundSet) {
          buf.append(" upper: ");
        } else {
          buf.append(" max: ");
        }
        buf.append(getUpperBound());
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StringStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      StringStatisticsImpl that = (StringStatisticsImpl) o;

      if (sum != that.sum) {
        return false;
      }
      if (minimum != null ? !minimum.equals(that.minimum) : that.minimum != null) {
        return false;
      }
      if (maximum != null ? !maximum.equals(that.maximum) : that.maximum != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (minimum != null ? minimum.hashCode() : 0);
      result = 31 * result + (maximum != null ? maximum.hashCode() : 0);
      result = 31 * result + (int) (sum ^ (sum >>> 32));
      return result;
    }

    private static void appendCodePoint(Text result, int codepoint) {
      if (codepoint < 0 || codepoint > 0x1f_ffff) {
        throw new IllegalArgumentException("Codepoint out of range " +
            codepoint);
      }
      byte[] buffer = new byte[4];
      if (codepoint < 0x7f) {
        buffer[0] = (byte) codepoint;
        result.append(buffer, 0, 1);
      } else if (codepoint <= 0x7ff) {
        buffer[0] = (byte) (0xc0 | (codepoint >> 6));
        buffer[1] = (byte) (0x80 | (codepoint & 0x3f));
        result.append(buffer, 0 , 2);
      } else if (codepoint < 0xffff) {
        buffer[0] = (byte) (0xe0 | (codepoint >> 12));
        buffer[1] = (byte) (0x80 | ((codepoint >> 6) & 0x3f));
        buffer[2] = (byte) (0x80 | (codepoint & 0x3f));
        result.append(buffer, 0, 3);
      } else {
        buffer[0] = (byte) (0xf0 | (codepoint >> 18));
        buffer[1] = (byte) (0x80 | ((codepoint >> 12) & 0x3f));
        buffer[2] = (byte) (0x80 | ((codepoint >> 6) & 0x3f));
        buffer[3] = (byte) (0x80 | (codepoint & 0x3f));
        result.append(buffer, 0, 4);
      }
    }

    /**
     * Create a text that is truncated to at most MAX_BYTES_RECORDED at a
     * character boundary with the last code point incremented by 1.
     * The length is assumed to be greater than MAX_BYTES_RECORDED.
     * @param text the text to truncate
     * @param from the index of the first character
     * @return truncated Text value
     */
    private static Text truncateUpperBound(final byte[] text, final int from) {
      int followingChar = Utf8Utils.findLastCharacter(text, from,
          from + MAX_BYTES_RECORDED);
      int lastChar = Utf8Utils.findLastCharacter(text, from, followingChar - 1);
      Text result = new Text();
      result.set(text, from, lastChar - from);
      appendCodePoint(result,
          Utf8Utils.getCodePoint(text, lastChar, followingChar - lastChar) + 1);
      return result;
    }

    /**
     * Create a text that is truncated to at most MAX_BYTES_RECORDED at a
     * character boundary.
     * The length is assumed to be greater than MAX_BYTES_RECORDED.
     * @param text Byte array to truncate
     * @param from This is the index of the first character
     * @return truncated {@link Text}
     */
    private static Text truncateLowerBound(final byte[] text, final int from) {

      int lastChar = Utf8Utils.findLastCharacter(text, from,
          from + MAX_BYTES_RECORDED);
      Text result = new Text();
      result.set(text, from, lastChar - from);
      return result;
    }
  }

  protected static final class BinaryStatisticsImpl extends ColumnStatisticsImpl implements
      BinaryColumnStatistics {

    private long sum = 0;

    BinaryStatisticsImpl() {
    }

    BinaryStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.BinaryStatistics binStats = stats.getBinaryStatistics();
      if (binStats.hasSum()) {
        sum = binStats.getSum();
      }
    }

    @Override
    public void reset() {
      super.reset();
      sum = 0;
    }

    @Override
    public void updateBinary(BytesWritable value) {
      sum += value.getLength();
    }

    @Override
    public void updateBinary(byte[] bytes, int offset, int length,
                             int repetitions) {
      sum += length * repetitions;
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof BinaryColumnStatistics) {
        BinaryStatisticsImpl bin = (BinaryStatisticsImpl) other;
        sum += bin.sum;
      } else {
        if (isStatsExists() && sum != 0) {
          throw new IllegalArgumentException("Incompatible merging of binary column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.BinaryStatistics.Builder bin = OrcProto.BinaryStatistics.newBuilder();
      bin.setSum(sum);
      result.setBinaryStatistics(bin);
      return result;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }
  }

  private static final class DecimalStatisticsImpl extends ColumnStatisticsImpl
      implements DecimalColumnStatistics {

    // These objects are mutable for better performance.
    private HiveDecimalWritable minimum = null;
    private HiveDecimalWritable maximum = null;
    private HiveDecimalWritable sum = new HiveDecimalWritable(0);

    DecimalStatisticsImpl() {
    }

    DecimalStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DecimalStatistics dec = stats.getDecimalStatistics();
      if (dec.hasMaximum()) {
        maximum = new HiveDecimalWritable(dec.getMaximum());
      }
      if (dec.hasMinimum()) {
        minimum = new HiveDecimalWritable(dec.getMinimum());
      }
      if (dec.hasSum()) {
        sum = new HiveDecimalWritable(dec.getSum());
      } else {
        sum = null;
      }
    }

    @Override
    public void reset() {
      super.reset();
      minimum = null;
      maximum = null;
      sum = new HiveDecimalWritable(0);
    }

    @Override
    public void updateDecimal(HiveDecimalWritable value) {
      if (minimum == null) {
        minimum = new HiveDecimalWritable(value);
        maximum = new HiveDecimalWritable(value);
      } else if (minimum.compareTo(value) > 0) {
        minimum.set(value);
      } else if (maximum.compareTo(value) < 0) {
        maximum.set(value);
      }
      if (sum != null) {
        sum.mutateAdd(value);
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof DecimalStatisticsImpl) {
        DecimalStatisticsImpl dec = (DecimalStatisticsImpl) other;
        if (minimum == null) {
          minimum = (dec.minimum != null ? new HiveDecimalWritable(dec.minimum) : null);
          maximum = (dec.maximum != null ? new HiveDecimalWritable(dec.maximum) : null);
          sum = dec.sum;
        } else if (dec.minimum != null) {
          if (minimum.compareTo(dec.minimum) > 0) {
            minimum.set(dec.minimum);
          }
          if (maximum.compareTo(dec.maximum) < 0) {
            maximum.set(dec.maximum);
          }
          if (sum == null || dec.sum == null) {
            sum = null;
          } else {
            sum.mutateAdd(dec.sum);
          }
        }
      } else {
        if (isStatsExists() && minimum != null) {
          throw new IllegalArgumentException("Incompatible merging of decimal column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.DecimalStatistics.Builder dec =
          OrcProto.DecimalStatistics.newBuilder();
      if (getNumberOfValues() != 0 && minimum != null) {
        dec.setMinimum(minimum.toString());
        dec.setMaximum(maximum.toString());
      }
      // Check isSet for overflow.
      if (sum != null && sum.isSet()) {
        dec.setSum(sum.toString());
      }
      result.setDecimalStatistics(dec);
      return result;
    }

    @Override
    public HiveDecimal getMinimum() {
      return minimum == null ? null : minimum.getHiveDecimal();
    }

    @Override
    public HiveDecimal getMaximum() {
      return maximum == null ? null : maximum.getHiveDecimal();
    }

    @Override
    public HiveDecimal getSum() {
      return sum == null ? null : sum.getHiveDecimal();
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
        if (sum != null) {
          buf.append(" sum: ");
          buf.append(sum);
        }
      }
      return buf.toString();
    }
  }

  private static final class DateStatisticsImpl extends ColumnStatisticsImpl
      implements DateColumnStatistics {
    private Integer minimum = null;
    private Integer maximum = null;

    DateStatisticsImpl() {
    }

    DateStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DateStatistics dateStats = stats.getDateStatistics();
      // min,max values serialized/deserialized as int (days since epoch)
      if (dateStats.hasMaximum()) {
        maximum = dateStats.getMaximum();
      }
      if (dateStats.hasMinimum()) {
        minimum = dateStats.getMinimum();
      }
    }

    @Override
    public void reset() {
      super.reset();
      minimum = null;
      maximum = null;
    }

    @Override
    public void updateDate(DateWritable value) {
      if (minimum == null) {
        minimum = value.getDays();
        maximum = value.getDays();
      } else if (minimum > value.getDays()) {
        minimum = value.getDays();
      } else if (maximum < value.getDays()) {
        maximum = value.getDays();
      }
    }

    @Override
    public void updateDate(int value) {
      if (minimum == null) {
        minimum = value;
        maximum = value;
      } else if (minimum > value) {
        minimum = value;
      } else if (maximum < value) {
        maximum = value;
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof DateStatisticsImpl) {
        DateStatisticsImpl dateStats = (DateStatisticsImpl) other;
        if (minimum == null) {
          minimum = dateStats.minimum;
          maximum = dateStats.maximum;
        } else if (dateStats.minimum != null) {
          if (minimum > dateStats.minimum) {
            minimum = dateStats.minimum;
          }
          if (maximum < dateStats.maximum) {
            maximum = dateStats.maximum;
          }
        }
      } else {
        if (isStatsExists() && minimum != null) {
          throw new IllegalArgumentException("Incompatible merging of date column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.DateStatistics.Builder dateStats =
          OrcProto.DateStatistics.newBuilder();
      if (getNumberOfValues() != 0 && minimum != null) {
        dateStats.setMinimum(minimum);
        dateStats.setMaximum(maximum);
      }
      result.setDateStatistics(dateStats);
      return result;
    }

    private transient final DateWritable minDate = new DateWritable();
    private transient final DateWritable maxDate = new DateWritable();

    @Override
    public Date getMinimum() {
      if (minimum == null) {
        return null;
      }
      minDate.set(minimum);
      return minDate.get();
    }

    @Override
    public Date getMaximum() {
      if (maximum == null) {
        return null;
      }
      maxDate.set(maximum);
      return maxDate.get();
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(getMinimum());
        buf.append(" max: ");
        buf.append(getMaximum());
      }
      return buf.toString();
    }
  }

  private static final class TimestampStatisticsImpl extends ColumnStatisticsImpl
      implements TimestampColumnStatistics {
    private Long minimum = null;
    private Long maximum = null;

    TimestampStatisticsImpl() {
    }

    TimestampStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.TimestampStatistics timestampStats = stats.getTimestampStatistics();
      // min,max values serialized/deserialized as int (milliseconds since epoch)
      if (timestampStats.hasMaximum()) {
        maximum = timestampStats.getMaximum();
      }
      if (timestampStats.hasMinimum()) {
        minimum = timestampStats.getMinimum();
      }
    }

    @Override
    public void reset() {
      super.reset();
      minimum = null;
      maximum = null;
    }

    @Override
    public void updateTimestamp(Timestamp value) {
      if (minimum == null) {
        minimum = value.getTime();
        maximum = value.getTime();
      } else if (minimum > value.getTime()) {
        minimum = value.getTime();
      } else if (maximum < value.getTime()) {
        maximum = value.getTime();
      }
    }

    @Override
    public void updateTimestamp(long value) {
      if (minimum == null) {
        minimum = value;
        maximum = value;
      } else if (minimum > value) {
        minimum = value;
      } else if (maximum < value) {
        maximum = value;
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof TimestampStatisticsImpl) {
        TimestampStatisticsImpl timestampStats = (TimestampStatisticsImpl) other;
        if (minimum == null) {
          minimum = timestampStats.minimum;
          maximum = timestampStats.maximum;
        } else if (timestampStats.minimum != null) {
          if (minimum > timestampStats.minimum) {
            minimum = timestampStats.minimum;
          }
          if (maximum < timestampStats.maximum) {
            maximum = timestampStats.maximum;
          }
        }
      } else {
        if (isStatsExists() && minimum != null) {
          throw new IllegalArgumentException("Incompatible merging of timestamp column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.TimestampStatistics.Builder timestampStats = OrcProto.TimestampStatistics
          .newBuilder();
      if (getNumberOfValues() != 0 && minimum != null) {
        timestampStats.setMinimum(minimum);
        timestampStats.setMaximum(maximum);
      }
      result.setTimestampStatistics(timestampStats);
      return result;
    }

    @Override
    public Timestamp getMinimum() {
      return minimum == null ? null : new Timestamp(minimum);
    }

    @Override
    public Timestamp getMaximum() {
      return maximum == null ? null : new Timestamp(maximum);
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(getMinimum());
        buf.append(" max: ");
        buf.append(getMaximum());
      }
      return buf.toString();
    }
  }

  private long count = 0;
  private boolean hasNull = false;

  ColumnStatisticsImpl(OrcProto.ColumnStatistics stats) {
    if (stats.hasNumberOfValues()) {
      count = stats.getNumberOfValues();
    }

    if (stats.hasHasNull()) {
      hasNull = stats.getHasNull();
    } else {
      hasNull = true;
    }
  }

  ColumnStatisticsImpl() {
  }

  public void increment() {
    count += 1;
  }

  public void increment(int count) {
    this.count += count;
  }

  public void setNull() {
    hasNull = true;
  }

  public void updateBoolean(boolean value, int repetitions) {
    throw new UnsupportedOperationException("Can't update boolean");
  }

  public void updateInteger(long value, int repetitions) {
    throw new UnsupportedOperationException("Can't update integer");
  }

  public void updateDouble(double value) {
    throw new UnsupportedOperationException("Can't update double");
  }

  public void updateString(Text value) {
    throw new UnsupportedOperationException("Can't update string");
  }

  public void updateString(byte[] bytes, int offset, int length,
                           int repetitions) {
    throw new UnsupportedOperationException("Can't update string");
  }

  public void updateBinary(BytesWritable value) {
    throw new UnsupportedOperationException("Can't update binary");
  }

  public void updateBinary(byte[] bytes, int offset, int length,
                           int repetitions) {
    throw new UnsupportedOperationException("Can't update string");
  }

  public void updateDecimal(HiveDecimalWritable value) {
    throw new UnsupportedOperationException("Can't update decimal");
  }

  public void updateDate(DateWritable value) {
    throw new UnsupportedOperationException("Can't update date");
  }

  public void updateDate(int value) {
    throw new UnsupportedOperationException("Can't update date");
  }

  public void updateTimestamp(Timestamp value) {
    throw new UnsupportedOperationException("Can't update timestamp");
  }

  public void updateTimestamp(long value) {
    throw new UnsupportedOperationException("Can't update timestamp");
  }

  public boolean isStatsExists() {
    return (count > 0 || hasNull == true);
  }

  public void merge(ColumnStatisticsImpl stats) {
    count += stats.count;
    hasNull |= stats.hasNull;
  }

  public void reset() {
    count = 0;
    hasNull = false;
  }

  @Override
  public long getNumberOfValues() {
    return count;
  }

  @Override
  public boolean hasNull() {
    return hasNull;
  }

  @Override
  public String toString() {
    return "count: " + count + " hasNull: " + hasNull;
  }

  public OrcProto.ColumnStatistics.Builder serialize() {
    OrcProto.ColumnStatistics.Builder builder =
      OrcProto.ColumnStatistics.newBuilder();
    builder.setNumberOfValues(count);
    builder.setHasNull(hasNull);
    return builder;
  }

  public static ColumnStatisticsImpl create(TypeDescription schema) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return new BooleanStatisticsImpl();
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new IntegerStatisticsImpl();
      case FLOAT:
      case DOUBLE:
        return new DoubleStatisticsImpl();
      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringStatisticsImpl();
      case DECIMAL:
        return new DecimalStatisticsImpl();
      case DATE:
        return new DateStatisticsImpl();
      case TIMESTAMP:
        return new TimestampStatisticsImpl();
      case BINARY:
        return new BinaryStatisticsImpl();
      default:
        return new ColumnStatisticsImpl();
    }
  }

  public static ColumnStatisticsImpl deserialize(OrcProto.ColumnStatistics stats) {
    if (stats.hasBucketStatistics()) {
      return new BooleanStatisticsImpl(stats);
    } else if (stats.hasIntStatistics()) {
      return new IntegerStatisticsImpl(stats);
    } else if (stats.hasDoubleStatistics()) {
      return new DoubleStatisticsImpl(stats);
    } else if (stats.hasStringStatistics()) {
      return new StringStatisticsImpl(stats);
    } else if (stats.hasDecimalStatistics()) {
      return new DecimalStatisticsImpl(stats);
    } else if (stats.hasDateStatistics()) {
      return new DateStatisticsImpl(stats);
    } else if (stats.hasTimestampStatistics()) {
      return new TimestampStatisticsImpl(stats);
    } else if(stats.hasBinaryStatistics()) {
      return new BinaryStatisticsImpl(stats);
    } else {
      return new ColumnStatisticsImpl(stats);
    }
  }
}
