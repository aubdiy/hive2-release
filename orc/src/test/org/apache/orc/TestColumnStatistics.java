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

package org.apache.orc;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.tools.FileDump;
import org.apache.orc.tools.TestFileDump;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test ColumnStatisticsImpl for ORC.
 */
public class TestColumnStatistics {

  @Test
  public void testLongMerge() throws Exception {
    TypeDescription schema = TypeDescription.createInt();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateInteger(10, 2);
    stats2.updateInteger(1, 1);
    stats2.updateInteger(1000, 1);
    stats1.merge(stats2);
    IntegerColumnStatistics typed = (IntegerColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum());
    assertEquals(1000, typed.getMaximum());
    stats1.reset();
    stats1.updateInteger(-10, 1);
    stats1.updateInteger(10000, 1);
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum());
    assertEquals(10000, typed.getMaximum());
  }

  @Test
  public void testDoubleMerge() throws Exception {
    TypeDescription schema = TypeDescription.createDouble();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateDouble(10.0);
    stats1.updateDouble(100.0);
    stats2.updateDouble(1.0);
    stats2.updateDouble(1000.0);
    stats1.merge(stats2);
    DoubleColumnStatistics typed = (DoubleColumnStatistics) stats1;
    assertEquals(1.0, typed.getMinimum(), 0.001);
    assertEquals(1000.0, typed.getMaximum(), 0.001);
    stats1.reset();
    stats1.updateDouble(-10);
    stats1.updateDouble(10000);
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum(), 0.001);
    assertEquals(10000, typed.getMaximum(), 0.001);
  }


  @Test
  public void testStringMerge() throws Exception {
    TypeDescription schema = TypeDescription.createString();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateString(new Text("bob"));
    stats1.updateString(new Text("david"));
    stats1.updateString(new Text("charles"));
    stats2.updateString(new Text("anne"));
    byte[] erin = new byte[]{0, 1, 2, 3, 4, 5, 101, 114, 105, 110};
    stats2.updateString(erin, 6, 4, 5);
    assertEquals(24, ((StringColumnStatistics)stats2).getSum());
    stats1.merge(stats2);
    StringColumnStatistics typed = (StringColumnStatistics) stats1;
    assertEquals("anne", typed.getMinimum());
    assertEquals("erin", typed.getMaximum());
    assertEquals(39, typed.getSum());
    stats1.reset();
    stats1.updateString(new Text("aaa"));
    stats1.updateString(new Text("zzz"));
    stats1.merge(stats2);
    assertEquals("aaa", typed.getMinimum());
    assertEquals("zzz", typed.getMaximum());
  }

  @Test
  public void testUpperAndLowerBounds() throws Exception {
    final TypeDescription schema = TypeDescription.createString();

    final String test = RandomStringUtils.random(1024+10);
    final String fragment = "foo"+test;
    final String fragmentLowerBound = "bar"+test;


    final ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    final ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);

    /* test a scenario for the first max string */
    stats1.updateString(new Text(test));

    final StringColumnStatistics typed = (StringColumnStatistics) stats1;
    final StringColumnStatistics typed2 = (StringColumnStatistics) stats2;

    assertTrue("Upperbound cannot be more than 1024 bytes",1024 >= typed.getUpperBound().getBytes().length);
    assertTrue("Lowerbound cannot be more than 1024 bytes",1024 >= typed.getLowerBound().getBytes().length);

    assertEquals(null, typed.getMinimum());
    assertEquals(null, typed.getMaximum());

    stats1.reset();

    /* test a scenario for the first max bytes */
    stats1.updateString(test.getBytes(), 0, test.getBytes().length, 0);

    assertTrue("Lowerbound cannot be more than 1024 bytes", 1024 >= typed.getLowerBound().getBytes().length);
    assertTrue("Upperbound cannot be more than 1024 bytes", 1024 >= typed.getUpperBound().getBytes().length);

    assertEquals(null, typed.getMinimum());
    assertEquals(null, typed.getMaximum());

    stats1.reset();
    /* test upper bound - merging  */
    stats1.updateString(new Text("bob"));
    stats1.updateString(new Text("david"));
    stats1.updateString(new Text("charles"));

    stats2.updateString(new Text("anne"));
    stats2.updateString(new Text(fragment));

    assertEquals("anne", typed2.getMinimum());
    assertEquals(null, typed2.getMaximum());

    stats1.merge(stats2);

    assertEquals("anne", typed.getMinimum());
    assertEquals(null, typed.getMaximum());


    /* test lower bound - merging  */
    stats1.reset();
    stats2.reset();

    stats1.updateString(new Text("david"));
    stats1.updateString(new Text("charles"));

    stats2.updateString(new Text("jane"));
    stats2.updateString(new Text(fragmentLowerBound));

    stats1.merge(stats2);

    assertEquals(null, typed.getMinimum());
    assertEquals("jane", typed.getMaximum());
  }

  /**
   * Test the string truncation with 1 byte characters. The last character
   * of the truncated string is 0x7f so that it will expand into a 2 byte
   * utf-8 character.
   */
  @Test
  public void testBoundsAscii() {
    StringBuilder buffer = new StringBuilder();
    for(int i=0; i < 256; ++i) {
      buffer.append("Owe\u007fn");
    }
    ColumnStatisticsImpl stats = ColumnStatisticsImpl.create(
        TypeDescription.createString());
    stats.increment();
    stats.updateString(new Text(buffer.toString()));
    StringColumnStatistics stringStats = (StringColumnStatistics) stats;

    // make sure that the min/max are null
    assertEquals(null, stringStats.getMinimum());
    assertEquals(null, stringStats.getMaximum());
    assertEquals(5 * 256, stringStats.getSum());

    // and that the lower and upper bound are correct
    assertEquals(buffer.substring(0, 1024), stringStats.getLowerBound());
    assertEquals("Owe\u0080", stringStats.getUpperBound().substring(1020));
    assertEquals("count: 1 hasNull: false lower: " + stringStats.getLowerBound()
            + " upper: " + stringStats.getUpperBound() + " sum: 1280",
        stringStats.toString());

    // make sure that when we replace the min & max the flags get cleared.
    stats.increment();
    stats.updateString(new Text("xxx"));
    assertEquals("xxx", stringStats.getMaximum());
    assertEquals("xxx", stringStats.getUpperBound());
    stats.increment();
    stats.updateString(new Text("A"));
    assertEquals("A", stringStats.getMinimum());
    assertEquals("A", stringStats.getLowerBound());
    assertEquals("count: 3 hasNull: false min: A max: xxx sum: 1284",
        stats.toString());
  }

  /**
   * Test truncation with 2 byte utf-8 characters.
   */
  @Test
  public void testBoundsTwoByte() {
    StringBuilder buffer = new StringBuilder();
    final String PATTERN = "\u0080\u07ff\u0432\u0246\u0123";
    for(int i=0; i < 256; ++i) {
      buffer.append(PATTERN);
    }
    ColumnStatisticsImpl stats = ColumnStatisticsImpl.create(
        TypeDescription.createString());
    stats.increment();
    stats.updateString(new Text(buffer.toString()));
    StringColumnStatistics stringStats = (StringColumnStatistics) stats;

    // make sure that the min/max are null
    assertEquals(null, stringStats.getMinimum());
    assertEquals(null, stringStats.getMaximum());
    assertEquals(2 * 5 * 256, stringStats.getSum());

    // and that the lower and upper bound are correct
    // 512 two byte characters fit in 1024 bytes
    assertEquals(buffer.substring(0, 512), stringStats.getLowerBound());
    assertEquals(buffer.substring(0, 511),
        stringStats.getUpperBound().substring(0, 511));
    assertEquals("\u0800", stringStats.getUpperBound().substring(511));
  }

  /**
   * Test truncation with 3 byte utf-8 characters.
   */
  @Test
  public void testBoundsThreeByte() {
    StringBuilder buffer = new StringBuilder();
    final String PATTERN = "\uffff\u0800\u4321\u1234\u3137";
    for(int i=0; i < 256; ++i) {
      buffer.append(PATTERN);
    }
    ColumnStatisticsImpl stats = ColumnStatisticsImpl.create(
        TypeDescription.createString());
    stats.increment();
    stats.updateString(new Text(buffer.toString()));
    StringColumnStatistics stringStats = (StringColumnStatistics) stats;

    // make sure that the min/max are null
    assertEquals(null, stringStats.getMinimum());
    assertEquals(null, stringStats.getMaximum());
    assertEquals(3 * 5 * 256, stringStats.getSum());

    // and that the lower and upper bound are correct
    // 341 three byte characters fit in 1024 bytes
    assertEquals(buffer.substring(0, 341), stringStats.getLowerBound());
    assertEquals(buffer.substring(0, 340),
        stringStats.getUpperBound().substring(0,340));
    assertEquals("\ud800\udc00", stringStats.getUpperBound().substring(340));
  }

  /**
   * Test truncation with 4 byte utf-8 characters.
   */
  @Test
  public void testBoundsFourByte() {
    StringBuilder buffer = new StringBuilder();
    final String PATTERN = "\ud800\udc00\ud801\udc01\ud802\udc02\ud803\udc03\ud804\udc04";
    for(int i=0; i < 256; ++i) {
      buffer.append(PATTERN);
    }
    ColumnStatisticsImpl stats = ColumnStatisticsImpl.create(
        TypeDescription.createString());
    stats.increment();
    stats.updateString(new Text(buffer.toString()));
    StringColumnStatistics stringStats = (StringColumnStatistics) stats;

    // make sure that the min/max are null
    assertEquals(null, stringStats.getMinimum());
    assertEquals(null, stringStats.getMaximum());
    assertEquals(4 * 5 * 256, stringStats.getSum());

    // and that the lower and upper bound are correct
    // 256 four byte characters fit in 1024 bytes
    assertEquals(buffer.substring(0, 512), stringStats.getLowerBound());
    assertEquals(buffer.substring(0, 510),
        stringStats.getUpperBound().substring(0, 510));
    assertEquals("\\uD800\\uDC01",
        StringEscapeUtils.escapeJava(stringStats.getUpperBound().substring(510)));
  }

  @Test
  public void testUpperBoundCodepointIncrement() {
    /* test with characters that use more than one byte */
    final String fragment =  "載記応存環敢辞月発併際岩。外現抱疑曲旧持九柏先済索。"
        + "富扁件戒程少交文相修宮由改価苦。位季供幾日本求知集機所江取号均下犯変第勝。"
        + "管今文図石職常暮海営感覧果賞挙。難加判郵年太願会周面市害成産。"
        + "内分載函取片領披見復来車必教。元力理関未法会伊団万球幕点帳幅為都話間。"
        + "親禁感栗合開注読月島月紀間卒派伏闘。幕経阿刊間都紹知禁追半業。"
        + "根案協話射格治位相機遇券外野何。話第勝平当降負京複掲書変痛。"
        + "博年群辺軽妻止和真権暑着要質在破応。"
        + "नीचे मुक्त बिन्दुओ समस्याओ आंतरकार्यक्षमता सुना प्रति सभीकुछ यायेका दिनांक वातावरण ";

    final String input = fragment
        + "मुश्किले केन्द्रिय "
        + "लगती नवंबर प्रमान गयेगया समस्याओ विश्व लिये समजते आपके एकत्रित विकेन्द्रित स्वतंत्र "
        + "व्याख्यान भेदनक्षमता शीघ्र होभर मुखय करता। दर्शाता वातावरण विस्तरणक्षमता दोषसके प्राप्त समाजो "
        + "।क तकनीकी दर्शाता कार्यकर्ता बाधा औषधिक समस्याओ समस्याए गोपनीयता प्राण पसंद "
        + "भीयह नवंबर दोषसके अनुवादक सोफ़तवेर समस्याए क्षमता। कार्य होभर\n";

    final String lowerBound = fragment +
        "मुश्किले केन्द्रिय लगती नवंबर प्रमान गयेगया समस्याओ विश्व लिये ";

    final String upperbound = fragment +
        "मुश्किले केन्द्रिय लगती नवंबर प्रमान गयेगया समस्याओ विश्व लिये!";

    final TypeDescription schema = TypeDescription.createString();
    final ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    byte[] utf8 = input.getBytes(StandardCharsets.UTF_8);
    stats1.updateString(utf8, 0, utf8.length, 1);

    final StringColumnStatistics typed = (StringColumnStatistics) stats1;

    assertEquals(354, typed.getUpperBound().length());
    assertEquals(354, typed.getLowerBound().length());

    assertEquals(upperbound, typed.getUpperBound());
    assertEquals(lowerBound, typed.getLowerBound());
  }

  @Test
  public void testDateMerge() throws Exception {
    TypeDescription schema = TypeDescription.createDate();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateDate(new DateWritable(1000));
    stats1.updateDate(new DateWritable(100));
    stats2.updateDate(new DateWritable(10));
    stats2.updateDate(new DateWritable(2000));
    stats1.merge(stats2);
    DateColumnStatistics typed = (DateColumnStatistics) stats1;
    assertEquals(new DateWritable(10).get(), typed.getMinimum());
    assertEquals(new DateWritable(2000).get(), typed.getMaximum());
    stats1.reset();
    stats1.updateDate(new DateWritable(-10));
    stats1.updateDate(new DateWritable(10000));
    stats1.merge(stats2);
    assertEquals(new DateWritable(-10).get(), typed.getMinimum());
    assertEquals(new DateWritable(10000).get(), typed.getMaximum());
  }

  @Test
  public void testTimestampMerge() throws Exception {
    TypeDescription schema = TypeDescription.createTimestamp();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateTimestamp(new Timestamp(10));
    stats1.updateTimestamp(new Timestamp(100));
    stats2.updateTimestamp(new Timestamp(1));
    stats2.updateTimestamp(new Timestamp(1000));
    stats1.merge(stats2);
    TimestampColumnStatistics typed = (TimestampColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum().getTime());
    assertEquals(1000, typed.getMaximum().getTime());
    stats1.reset();
    stats1.updateTimestamp(new Timestamp(-10));
    stats1.updateTimestamp(new Timestamp(10000));
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum().getTime());
    assertEquals(10000, typed.getMaximum().getTime());
  }

  @Test
  public void testDecimalMerge() throws Exception {
    TypeDescription schema = TypeDescription.createDecimal()
        .withPrecision(38).withScale(16);

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateDecimal(new HiveDecimalWritable(10));
    stats1.updateDecimal(new HiveDecimalWritable(100));
    stats2.updateDecimal(new HiveDecimalWritable(1));
    stats2.updateDecimal(new HiveDecimalWritable(1000));
    stats1.merge(stats2);
    DecimalColumnStatistics typed = (DecimalColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum().longValue());
    assertEquals(1000, typed.getMaximum().longValue());
    stats1.reset();
    stats1.updateDecimal(new HiveDecimalWritable(-10));
    stats1.updateDecimal(new HiveDecimalWritable(10000));
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum().longValue());
    assertEquals(10000, typed.getMaximum().longValue());
  }


  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("TestOrcFile." + testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  private static BytesWritable bytes(int... items) {
    BytesWritable result = new BytesWritable();
    result.setSize(items.length);
    for (int i = 0; i < items.length; ++i) {
      result.getBytes()[i] = (byte) items[i];
    }
    return result;
  }

  void appendRow(VectorizedRowBatch batch, BytesWritable bytes,
                 String str) {
    int row = batch.size++;
    if (bytes == null) {
      batch.cols[0].noNulls = false;
      batch.cols[0].isNull[row] = true;
    } else {
      ((BytesColumnVector) batch.cols[0]).setVal(row, bytes.getBytes(),
          0, bytes.getLength());
    }
    if (str == null) {
      batch.cols[1].noNulls = false;
      batch.cols[1].isNull[row] = true;
    } else {
      ((BytesColumnVector) batch.cols[1]).setVal(row, str.getBytes());
    }
  }

  @Test
  public void testHasNull() throws Exception {
    TypeDescription schema =
        TypeDescription.createStruct()
        .addField("bytes1", TypeDescription.createBinary())
        .addField("string1", TypeDescription.createString());
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(1000)
            .stripeSize(10000)
            .bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch(5000);
    // STRIPE 1
    // RG1
    for(int i=0; i<1000; i++) {
      appendRow(batch, bytes(1, 2, 3), "RG1");
    }
    writer.addRowBatch(batch);
    batch.reset();
    // RG2
    for(int i=0; i<1000; i++) {
      appendRow(batch, bytes(1, 2, 3), null);
    }
    writer.addRowBatch(batch);
    batch.reset();
    // RG3
    for(int i=0; i<1000; i++) {
      appendRow(batch, bytes(1, 2, 3), "RG3");
    }
    writer.addRowBatch(batch);
    batch.reset();
    // RG4
    for (int i = 0; i < 1000; i++) {
      appendRow(batch, bytes(1,2,3), null);
    }
    writer.addRowBatch(batch);
    batch.reset();
    // RG5
    for(int i=0; i<1000; i++) {
      appendRow(batch, bytes(1, 2, 3), null);
    }
    writer.addRowBatch(batch);
    batch.reset();
    // STRIPE 2
    for (int i = 0; i < 5000; i++) {
      appendRow(batch, bytes(1,2,3), null);
    }
    writer.addRowBatch(batch);
    batch.reset();
    // STRIPE 3
    for (int i = 0; i < 5000; i++) {
      appendRow(batch, bytes(1,2,3), "STRIPE-3");
    }
    writer.addRowBatch(batch);
    batch.reset();
    // STRIPE 4
    for (int i = 0; i < 5000; i++) {
      appendRow(batch, bytes(1,2,3), null);
    }
    writer.addRowBatch(batch);
    batch.reset();
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    // check the file level stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(20000, stats[0].getNumberOfValues());
    assertEquals(20000, stats[1].getNumberOfValues());
    assertEquals(7000, stats[2].getNumberOfValues());
    assertEquals(false, stats[0].hasNull());
    assertEquals(false, stats[1].hasNull());
    assertEquals(true, stats[2].hasNull());

    // check the stripe level stats
    List<StripeStatistics> stripeStats = reader.getStripeStatistics();
    // stripe 1 stats
    StripeStatistics ss1 = stripeStats.get(0);
    ColumnStatistics ss1_cs1 = ss1.getColumnStatistics()[0];
    ColumnStatistics ss1_cs2 = ss1.getColumnStatistics()[1];
    ColumnStatistics ss1_cs3 = ss1.getColumnStatistics()[2];
    assertEquals(false, ss1_cs1.hasNull());
    assertEquals(false, ss1_cs2.hasNull());
    assertEquals(true, ss1_cs3.hasNull());

    // stripe 2 stats
    StripeStatistics ss2 = stripeStats.get(1);
    ColumnStatistics ss2_cs1 = ss2.getColumnStatistics()[0];
    ColumnStatistics ss2_cs2 = ss2.getColumnStatistics()[1];
    ColumnStatistics ss2_cs3 = ss2.getColumnStatistics()[2];
    assertEquals(false, ss2_cs1.hasNull());
    assertEquals(false, ss2_cs2.hasNull());
    assertEquals(true, ss2_cs3.hasNull());

    // stripe 3 stats
    StripeStatistics ss3 = stripeStats.get(2);
    ColumnStatistics ss3_cs1 = ss3.getColumnStatistics()[0];
    ColumnStatistics ss3_cs2 = ss3.getColumnStatistics()[1];
    ColumnStatistics ss3_cs3 = ss3.getColumnStatistics()[2];
    assertEquals(false, ss3_cs1.hasNull());
    assertEquals(false, ss3_cs2.hasNull());
    assertEquals(false, ss3_cs3.hasNull());

    // stripe 4 stats
    StripeStatistics ss4 = stripeStats.get(3);
    ColumnStatistics ss4_cs1 = ss4.getColumnStatistics()[0];
    ColumnStatistics ss4_cs2 = ss4.getColumnStatistics()[1];
    ColumnStatistics ss4_cs3 = ss4.getColumnStatistics()[2];
    assertEquals(false, ss4_cs1.hasNull());
    assertEquals(false, ss4_cs2.hasNull());
    assertEquals(true, ss4_cs3.hasNull());

    // Test file dump
    PrintStream origOut = System.out;
    String outputFilename = "orc-file-has-null.out";
    FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "--rowindex=2"});
    System.out.flush();
    System.setOut(origOut);
    // If called with an expression evaluating to false, the test will halt
    // and be ignored.
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));
    TestFileDump.checkOutput(outputFilename, workDir + File.separator + outputFilename);
  }
}
