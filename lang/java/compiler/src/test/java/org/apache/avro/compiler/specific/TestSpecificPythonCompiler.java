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
package org.apache.avro.compiler.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.equalTo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.AvroTestUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.StringType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;

@RunWith(JUnit4.class)
public class TestSpecificPythonCompiler {
  private final String schemaSrcPath = "src/test/resources/simple_python_record.avsc";
  private final String velocityTemplateDir =
      "src/main/velocity/org/apache/avro/compiler/specific/templates/python/classic/";
  private File src;
  private File outputDir;
  private File outputFile;
  private File initFile;
  private String namespaceFolder = "org/apache/avro/compiler/tests";
  private List<String> lines = new ArrayList<String>();

  @Before
  public void setUp() throws IOException {
    this.src = new File(this.schemaSrcPath);
    this.outputDir = AvroTestUtil.tempDirectory(getClass(), "specific-output");
    this.initFile = new File(Paths.get(this.outputDir.toString(), "__init__.py").toString());
    this.outputFile = new File(Paths.get(this.outputDir.toString(), namespaceFolder, "SimpleRecord.py").toString());
    if (outputFile.exists() && !outputFile.delete()) {
      throw new IllegalStateException("unable to delete " + outputFile);
    }
    SpecificPythonCompiler compiler = createCompiler();
    compiler.compileToDestination(this.src, this.outputDir);
    BufferedReader reader = new BufferedReader(new FileReader(this.outputFile));
    String line = null;
    while ((line = reader.readLine()) != null) {
      this.lines.add(line.trim());
    }
    reader.close();
  }

  @After
  public void tearDow() {
    if (this.outputFile != null) {
      this.outputFile.delete();
    }
  }

  /** Uses the system's java compiler to actually compile the generated code. */
  static void assertCompilesWithJavaCompiler(Collection<SpecificPythonCompiler.OutputFile> outputs)
          throws IOException {
    if (outputs.isEmpty())
      return;               // Nothing to compile!

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fileManager =
            compiler.getStandardFileManager(null, null, null);

    File dstDir = AvroTestUtil.tempFile(TestSpecificPythonCompiler.class, "realCompiler");
    List<File> javaFiles = new ArrayList<File>();
    for (SpecificPythonCompiler.OutputFile o : outputs) {
      javaFiles.add(o.writeToDestination(null, dstDir));
    }

    JavaCompiler.CompilationTask cTask = compiler.getTask(null, fileManager,
            null, null, null, fileManager.getJavaFileObjects(
                    javaFiles.toArray(new File[javaFiles.size()])));
    boolean compilesWithoutError = cTask.call();
    assertTrue(compilesWithoutError);
  }

  private static Schema createSampleRecordSchema(int numStringFields, int numDoubleFields) {
    SchemaBuilder.FieldAssembler<Schema> sb = SchemaBuilder.record("sample.record").fields();
    for (int i = 0; i < numStringFields; i++) {
      sb.name("sf_" + i).type().stringType().noDefault();
    }
    for (int i = 0; i < numDoubleFields; i++) {
      sb.name("df_" + i).type().doubleType().noDefault();
    }
    return sb.endRecord();
  }

  private SpecificPythonCompiler createCompiler() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(this.src);
    SpecificPythonCompiler compiler = new SpecificPythonCompiler(schema);
    compiler.setTemplateDir(this.velocityTemplateDir);
    compiler.setStringType(StringType.CharSequence);
    return compiler;
  }

  @Test
  public void testCanReadTemplateFilesOnTheFilesystem() throws IOException, URISyntaxException{
    assertTrue(this.outputFile.exists());
    assertTrue(this.initFile.exists());
  }

  @Test
  public void testForDocHeader() throws IOException {
      Boolean available = false;
      for(String line: this.lines) {
          if (line.startsWith("##")) {
              // Check if the AUTOGENERATED comment is available
              if (line.matches(".*Autogenerated.*(?i)")) {
                  available = true;
                  break;
              }
          }
      }
      assertTrue(available);
  }

  @Test
  public void testForImports() {
      Pattern pattern = Pattern.compile("from (.*) import.*");
      ArrayList<String> imports = new ArrayList<String>();
      imports.add("elasticsearch_dsl");
      imports.add("avro");
      int count = 0;

      for(String line: this.lines) {
          if (line.startsWith("from")) {
              Matcher matcher = pattern.matcher(line);
              while(matcher.find()) {
                  if (imports.contains(matcher.group(1))) {
                      count += 1;
                  }
              }
          }
      }
      // 3 imports
      assertTrue(count == 3);
  }

  @Test
  public void testForDataTypes() {
      Pattern pattern = Pattern.compile("\\s*(\\w+)\\s*=\\s*(\\w+)\\(.*\\).*");
      HashMap<String, String> map = new HashMap<String, String>();
      map.put("name", "Text");
      map.put("count", "Integer");
      map.put("float", "Float");
      map.put("bytes", "Byte");
      map.put("bool", "Boolean");

      for(String line: this.lines) {
          Matcher matcher = pattern.matcher(line);
          while(matcher.find()) {
              assertTrue(matcher.group(2).equals(map.get(matcher.group(1))));
          }
      }
  }

  @Test
  public void testInitImports() throws IOException {
      BufferedReader reader = new BufferedReader(new FileReader(this.initFile));
      String line = null;
      Boolean available = false;
      while ((line = reader.readLine()) != null) {
          if (line.trim().equals("from .org.apache.avro.compiler.tests.SimpleRecord import SimpleRecord")) {
              available = true;
              break;
          }
      }
      reader.close();
      assertTrue(available);
  }

  @Test
  public void testInitFilesInFolders() {
      String exts[] = {"py"};
      Collection<File> files = FileUtils.listFiles(this.initFile.getParentFile(), exts, true);
      int count = 0;
      for(File f: files) {
          String fname = f.toString();
          if(fname.matches(".*__init__.py.*")) {
              // Count the numbers
              count += 1;
          }
      }
      // We need to assert if there are files as per namespace
      // One on top, one per namespace,
      assertTrue(count == this.namespaceFolder.split("/").length + 1);
  }
}
