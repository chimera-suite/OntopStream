package it.unibz.inf.ontop.cli;

import java.io.BufferedWriter;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicLong;

public class PeriodicFlushingBufferedWriter extends Writer {

    protected final MonitoredWriter monitoredWriter;
    protected final BufferedWriter writer;

    protected final long timeout;
    protected final Thread thread;

    public PeriodicFlushingBufferedWriter(Writer out, long timeout) {
        this(out, 8192, timeout);
    }

    public PeriodicFlushingBufferedWriter(Writer out, int sz, final long timeout) {
        monitoredWriter = new MonitoredWriter(out);
        writer = new BufferedWriter(monitoredWriter, sz);

        this.timeout = timeout;

        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                long deadline = System.currentTimeMillis() + timeout;
                while (!Thread.interrupted()) {
                    try {
                        Thread.sleep(Math.max(deadline - System.currentTimeMillis(), 0));
                    } catch (InterruptedException e) {
                        return;
                    }

                    synchronized (PeriodicFlushingBufferedWriter.this) {
                        if (Thread.interrupted()) {
                            return;
                        }

                        long lastWrite = monitoredWriter.getLastWrite();

                        if (System.currentTimeMillis() - lastWrite >= timeout) {
                            try {
                                writer.flush();
                            } catch (IOException e) {
                            }
                        }

                        deadline = lastWrite + timeout;
                    }
                }
            }
        });

        thread.start();
    }

    @Override
    public synchronized void write(char[] cbuf, int off, int len) throws IOException {
        this.writer.write(cbuf, off, len);
    }

    public void newLine() throws IOException {
        this.writer.newLine();
    }

    @Override
    public synchronized void flush() throws IOException {
        this.writer.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            thread.interrupt();
        } finally {
            this.writer.close();
        }
    }

    private static class MonitoredWriter extends FilterWriter {

        protected final AtomicLong lastWrite = new AtomicLong();

        protected MonitoredWriter(Writer out) {
            super(out);
        }

        @Override
        public void write(int c) throws IOException {
            lastWrite.set(System.currentTimeMillis());
            super.write(c);
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            lastWrite.set(System.currentTimeMillis());
            super.write(cbuf, off, len);
        }

        @Override
        public void write(String str, int off, int len) throws IOException {
            lastWrite.set(System.currentTimeMillis());
            super.write(str, off, len);
        }

        @Override
        public void flush() throws IOException {
            lastWrite.set(System.currentTimeMillis());
            super.flush();
        }

        public long getLastWrite() {
            return this.lastWrite.get();
        }
    }
}
