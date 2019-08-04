package org.apache.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.PipelineTask.PipelineWork;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestPipelineTask {
  private final Configuration conf = new Configuration();
  private final IOException ioe = new IOException();

  private class TestItem {
    private volatile int val;

    public TestItem(int i) {
      val = i;
    }

    public int getVal() {
      return val;
    }

    public void setVal(int inVal) {
      val = inVal;
    }
  }

  private void runTest(int level, final boolean exp) throws Throwable {
    final List<TestItem> items = new ArrayList<TestItem>();
    for (int i = 0; i < 1000; i++) {
      items.add(new TestItem(i));
    }
    final Object EMPTY = new Object();
    PipelineWork header = new PipelineWork<Object, TestItem>() {
      int idx = 0;

      @Override
      public TestItem doWork(Object obj) throws IOException {
        if (idx < items.size()) {
          TestItem item = items.get(idx);
          item.setVal(item.getVal() * 2);
          if (exp && (idx == items.size() - 1)) {
            throw ioe;
          }
          idx++;
          return item;
        } else {
          return null;
        }
      }
    };
    PipelineWork middle = new PipelineWork<TestItem, TestItem>() {
      @Override
      public TestItem doWork(TestItem item) {
        item.setVal(item.getVal() * 2);
        return item;
      }
    };
    PipelineWork trailer = new PipelineWork<TestItem, Object>() {
      @Override
      public Object doWork(TestItem item) {
        item.setVal(item.getVal() * 2);
        return EMPTY;
      }
    };
    PipelineTask task = new PipelineTask(conf, "pipeline.testcarriage");
    if (level <= 0) {
      return;
    } else if (level == 1) {
      task.appendWork(header, "header");
      task.kickOff();
      task.join();
    } else if (level == 2) {
      task.appendWork(header, "header").appendWork(trailer, "trailer");
      task.kickOff();
      task.join();
    } else {
      task.appendWork(header, "header");
      for (int i = 0; i < level - 2; i++) {
        task.appendWork(middle, "middle" + i);
      }
      task.appendWork(trailer, "trailer");
      task.kickOff();
      task.join();
    }
    if (task.getException() != null) {
      throw task.getException();
    }
    for (int i = 0; i < items.size(); i++) {
      Assert.assertTrue(items.get(i).getVal() == i * Math.pow(2, level));
    }
  }

  @Test
  public void basicTests() throws Throwable {
    for (int i = 1; i < 10; i++) {
      runTest(i, false);
    }
  }

  @Test
  public void testException() throws InterruptedException {
    try {
      runTest(3, true);
    } catch (Throwable t) {
      Assert.assertTrue(t == ioe);
      return;
    }
    Assert.assertTrue(false);
  }
}
