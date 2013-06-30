# Snappy in Java

This is a rewrite (port) of [Snappy](http://code.google.com/p/snappy/) writen in
pure Java. This compression code produces a byte-for-byte exact copy of the output
created by the original C++ code, and extremely fast.

# Performance

The Snappy micro-benchmark has been ported, and can be used to measure
the performance of this code against the excellent Snappy JNI wrapper from
[xerial](http://code.google.com/p/snappy-java/).  As you can see in the results
below, the pure Java port is 20-30% faster for block compress, 0-10% slower
for block uncompress, and 0-5% slower for round-trip block compression.  These
results were run with Java 7 on a Core i7, 64-bit Mac.

As a second more independent test, the performance has been measured using the
Ning JVM compression benchmark against Snappy JNI, and the pure Java
[Ning LZF](https://github.com/ning/compress) codec. The
[results](http://dain.github.com/snappy/) show that the pure Java Snappy is
20-30% faster than JNI Snappy for compression, and is typically 10-20% slower
for decompression. Both, the pure Java Snappy and JNI Snappy implementations
are faster that the Ning LZF codec.  These results were run with Java 6 on a
Core i7, 64-bit Mac.

The difference in performance between these two tests is due to the difference
in JVM version;  Java 7 is consistently 5-10% faster than Java 6 in the
compression code. As with all benchmarks your mileage will vary, so test with
your actual use case.



### Block Compress
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   294.9MB/s   384.8MB/s  +30.5%  html
urls       702087     49.1%     49.1%   178.7MB/s   226.5MB/s  +26.8%  urls
jpg        126958      0.1%      0.1%     2.7GB/s     3.2GB/s  +17.4%  jpg (not compressible)
pdf         94330     17.9%     17.9%   642.4MB/s   910.3MB/s  +41.7%  pdf
html4      409600     76.4%     76.4%   289.2MB/s   377.3MB/s  +30.5%  html4
cp          24603     51.9%     51.9%   166.4MB/s   233.7MB/s  +40.5%  cp
c           11150     57.6%     57.6%   177.1MB/s   295.4MB/s  +66.8%  c
lsp          3721     51.6%     51.6%   245.5MB/s   278.0MB/s  +13.2%  lsp
xls       1029744     58.7%     58.7%   263.0MB/s   292.5MB/s  +11.2%  xls
txt1       152089     40.2%     40.2%   116.8MB/s   163.1MB/s  +39.7%  txt1
txt2       125179     35.9%     35.9%   112.5MB/s   153.4MB/s  +36.3%  txt2
txt3       426754     42.9%     42.9%   123.3MB/s   169.8MB/s  +37.6%  txt3
txt4       481861     31.7%     31.7%   107.8MB/s   146.2MB/s  +35.6%  txt4
bin        513216     81.8%     81.8%   413.1MB/s   497.8MB/s  +20.5%  bin
sum         38240     48.1%     48.1%   162.4MB/s   213.9MB/s  +31.7%  sum
man          4227     40.6%     40.6%   194.6MB/s   241.7MB/s  +24.2%  man
pb         118588     76.8%     76.8%   363.7MB/s   450.3MB/s  +23.8%  pb
gaviota    184320     61.7%     61.7%   166.7MB/s   253.7MB/s  +52.2%  gaviota
</code></pre>


### Block Uncompress
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%     1.5GB/s     1.3GB/s  -12.2%  html
urls       702087     49.1%     49.1%   969.2MB/s   827.5MB/s  -14.6%  urls
jpg        126958      0.1%      0.1%    18.6GB/s    19.4GB/s   +4.2%  jpg (not compressible)
pdf         94330     17.9%     17.9%     4.1GB/s     3.7GB/s   -8.8%  pdf
html4      409600     76.4%     76.4%     1.5GB/s     1.2GB/s  -16.8%  html4
cp          24603     51.9%     51.9%   965.2MB/s   956.0MB/s   -1.0%  cp
c           11150     57.6%     57.6%   989.1MB/s   924.9MB/s   -6.5%  c
lsp          3721     51.6%     51.6%   991.6MB/s   964.8MB/s   -2.7%  lsp
xls       1029744     58.7%     58.7%   798.4MB/s   747.3MB/s   -6.4%  xls
txt1       152089     40.2%     40.2%   643.8MB/s   580.8MB/s   -9.8%  txt1
txt2       125179     35.9%     35.9%   610.0MB/s   549.6MB/s   -9.9%  txt2
txt3       426754     42.9%     42.9%   683.8MB/s   614.4MB/s  -10.2%  txt3
txt4       481861     31.7%     31.7%   565.4MB/s   505.5MB/s  -10.6%  txt4
bin        513216     81.8%     81.8%     1.5GB/s     1.2GB/s  -20.4%  bin
sum         38240     48.1%     48.1%   838.1MB/s   771.6MB/s   -7.9%  sum
man          4227     40.6%     40.6%   856.9MB/s   847.2MB/s   -1.1%  man
pb         118588     76.8%     76.8%     1.7GB/s     1.5GB/s  -12.9%  pb
gaviota    184320     61.7%     61.7%   769.1MB/s   693.4MB/s   -9.9%  gaviota
</code></pre>


### Block Round Trip
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   300.3MB/s   287.1MB/s   -4.4%  html
urls       702087     49.1%     49.1%   182.7MB/s   177.0MB/s   -3.2%  urls
jpg        126958      0.1%      0.1%     2.6GB/s     2.6GB/s   +1.1%  jpg (not compressible)
pdf         94330     17.9%     17.9%   695.3MB/s   680.0MB/s   -2.2%  pdf
html4      409600     76.4%     76.4%   296.4MB/s   282.1MB/s   -4.8%  html4
cp          24603     51.9%     51.9%   177.0MB/s   172.5MB/s   -2.5%  cp
c           11150     57.6%     57.6%   221.7MB/s   218.3MB/s   -1.5%  c
lsp          3721     51.6%     51.6%   217.3MB/s   216.3MB/s   -0.5%  lsp
xls       1029744     58.7%     58.7%   213.3MB/s   209.9MB/s   -1.6%  xls
txt1       152089     40.2%     40.2%   129.4MB/s   126.3MB/s   -2.4%  txt1
txt2       125179     35.9%     35.9%   121.7MB/s   118.8MB/s   -2.4%  txt2
txt3       426754     42.9%     42.9%   135.2MB/s   132.8MB/s   -1.8%  txt3
txt4       481861     31.7%     31.7%   115.2MB/s   113.0MB/s   -1.9%  txt4
bin        513216     81.8%     81.8%   371.2MB/s   350.7MB/s   -5.5%  bin
sum         38240     48.1%     48.1%   164.2MB/s   160.0MB/s   -2.6%  sum
man          4227     40.6%     40.6%   184.8MB/s   185.3MB/s   +0.3%  man
pb         118588     76.8%     76.8%   344.1MB/s   326.3MB/s   -5.2%  pb
gaviota    184320     61.7%     61.7%   188.0MB/s   185.2MB/s   -1.5%  gaviota
</code></pre>

# Stream Format

There is no defined stream format for Snappy, but there is an effort to create
a common format with the Google Snappy project.

The stream format used in this library has a couple of unique features not
found in the other Snappy stream formats.  Like the other formats, the user
input is broken into blocks and each block is compressed.  If the compressed
block is smaller that the user input, the compressed block is written,
otherwise the uncompressed original is written.  This dramatically improves the
speed of uncompressible input such as JPG images.  Additionally, a checksum of
the user input data for each block is written to the stream.  This safety check
assures that the stream has not been corrupted in transit or by a bad Snappy
implementation.  Finally, like gzip, compressed Snappy files can be
concatenated together without issue, since the input stream will ignore a
Snappy stream header in the middle of a stream.  This makes combining files in
Hadoop and S3 trivial.

The the SnappyOutputStream javadocs contain formal definition of the stream
format.

## Stream Performance

The streaming mode performance can not be directly compared to other
compression algorithms since most formats do not contain a checksum.  The basic
streaming code is significantly faster that the Snappy JNI library due to
the completely unoptimized stream implementation in Snappy JNI, but once the
check sum is enabled the performance drops off by about 20%.

### Stream Compress (no checksums)
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   275.8MB/s   373.5MB/s  +35.4%  html
urls       702087     49.1%     49.1%   176.5MB/s   225.2MB/s  +27.6%  urls
jpg        126958      0.1%     -0.0%     1.7GB/s     2.0GB/s  +15.8%  jpg (not compressible)
pdf         94330     17.8%     16.0%   557.2MB/s   793.2MB/s  +42.4%  pdf
html4      409600     76.4%     76.4%   281.0MB/s   369.9MB/s  +31.7%  html4
cp          24603     51.8%     51.8%   151.7MB/s   214.3MB/s  +41.3%  cp
c           11150     57.4%     57.5%   149.1MB/s   243.3MB/s  +63.1%  c
lsp          3721     51.1%     51.2%   141.3MB/s   181.1MB/s  +28.2%  lsp
xls       1029744     58.6%     58.6%   253.9MB/s   290.5MB/s  +14.4%  xls
txt1       152089     40.2%     40.2%   114.8MB/s   159.4MB/s  +38.8%  txt1
txt2       125179     35.9%     35.9%   110.0MB/s   150.4MB/s  +36.7%  txt2
txt3       426754     42.9%     42.9%   121.0MB/s   167.9MB/s  +38.8%  txt3
txt4       481861     31.6%     31.6%   105.1MB/s   143.2MB/s  +36.2%  txt4
bin        513216     81.8%     81.8%   387.7MB/s   484.5MB/s  +25.0%  bin
sum         38240     48.1%     48.1%   153.0MB/s   203.1MB/s  +32.8%  sum
man          4227     40.2%     40.3%   125.9MB/s   171.9MB/s  +36.5%  man
pb         118588     76.8%     76.8%   342.2MB/s   431.4MB/s  +26.1%  pb
gaviota    184320     61.7%     61.7%   161.1MB/s   246.1MB/s  +52.7%  gaviota
</code></pre>


### Stream Uncompress (no checksums)
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%     1.2GB/s     1.2GB/s   +0.4%  html
urls       702087     49.1%     49.1%   853.9MB/s   786.6MB/s   -7.9%  urls
jpg        126958      0.1%     -0.0%     3.0GB/s    10.3GB/s +239.0%  jpg (not compressible)
pdf         94330     17.8%     16.0%     2.0GB/s     3.4GB/s  +71.5%  pdf
html4      409600     76.4%     76.4%     1.2GB/s     1.1GB/s   -8.4%  html4
cp          24603     51.8%     51.8%   785.2MB/s   905.6MB/s  +15.3%  cp
c           11150     57.4%     57.5%   778.9MB/s   889.7MB/s  +14.2%  c
lsp          3721     51.1%     51.2%   739.0MB/s   905.5MB/s  +22.5%  lsp
xls       1029744     58.6%     58.6%   730.3MB/s   718.8MB/s   -1.6%  xls
txt1       152089     40.2%     40.2%   582.4MB/s   559.0MB/s   -4.0%  txt1
txt2       125179     35.9%     35.9%   540.7MB/s   526.4MB/s   -2.6%  txt2
txt3       426754     42.9%     42.9%   620.5MB/s   583.9MB/s   -5.9%  txt3
txt4       481861     31.6%     31.6%   519.4MB/s   487.0MB/s   -6.2%  txt4
bin        513216     81.8%     81.8%     1.2GB/s     1.1GB/s  -11.6%  bin
sum         38240     48.1%     48.1%   693.4MB/s   742.4MB/s   +7.1%  sum
man          4227     40.2%     40.3%   637.3MB/s   784.3MB/s  +23.1%  man
pb         118588     76.8%     76.8%     1.4GB/s     1.4GB/s   +0.4%  pb
gaviota    184320     61.7%     61.7%   688.5MB/s   668.2MB/s   -3.0%  gaviota
</code></pre>


### Stream RoundTrip (no checksums)
<pre><code>
                        JNI      Java         JNI        Java
Input        Size  Compress  Compress  Throughput  Throughput  Change
---------------------------------------------------------------------
html       102400     76.4%     76.4%   223.8MB/s   272.5MB/s  +21.8%  html
urls       702087     49.1%     49.1%   142.8MB/s   174.1MB/s  +22.0%  urls
jpg        126958      0.1%     -0.0%     1.1GB/s     1.6GB/s  +52.1%  jpg (not compressible)
pdf         94330     17.8%     16.0%   421.9MB/s   610.1MB/s  +44.6%  pdf
html4      409600     76.4%     76.4%   226.2MB/s   275.5MB/s  +21.8%  html4
cp          24603     51.8%     51.8%   125.3MB/s   160.3MB/s  +27.9%  cp
c           11150     57.4%     57.5%   125.1MB/s   183.2MB/s  +46.5%  c
lsp          3721     51.1%     51.2%   130.6MB/s   149.5MB/s  +14.5%  lsp
xls       1029744     58.6%     58.6%   188.2MB/s   206.1MB/s   +9.5%  xls
txt1       152089     40.2%     40.2%    95.3MB/s   123.3MB/s  +29.4%  txt1
txt2       125179     35.9%     35.9%    91.4MB/s   116.8MB/s  +27.9%  txt2
txt3       426754     42.9%     42.9%   101.3MB/s   130.3MB/s  +28.6%  txt3
txt4       481861     31.6%     31.6%    87.9MB/s   111.1MB/s  +26.3%  txt4
bin        513216     81.8%     81.8%   294.7MB/s   337.9MB/s  +14.7%  bin
sum         38240     48.1%     48.1%   122.9MB/s   152.9MB/s  +24.3%  sum
man          4227     40.2%     40.3%   113.0MB/s   139.1MB/s  +23.1%  man
pb         118588     76.8%     76.8%   269.5MB/s   313.8MB/s  +16.4%  pb
gaviota    184320     61.7%     61.7%   131.1MB/s   180.3MB/s  +37.6%  gaviota
</code></pre>
