import 'dart:io';
import 'package:http/http.dart' as http;
import 'package:path_provider/path_provider.dart';
import 'package:youtube_explode_dart/youtube_explode_dart.dart';

class DownloadEngine {
  // دالة رئيسية لبدء التحميل المقسم إلى 32 جزءاً
  static Future<void> downloadVideo({
    required String videoUrl,
    required String title,
    required Function(double progress, String speed) onProgress,
    required Function() onComplete,
  }) async {
    final yt = (YoutubeExplode);
    try {
      // 1. جلب بيانات تدفق الفيديو وأعلى جودة متاحة بشكل صحيح
      var manifest = await yt.videos.streamsClient.getManifest(videoUrl);
      var streamInfo = manifest.muxed.sortByBitrate().last;
      
      var fileUrl = streamInfo.url.toString();
      var totalSize = streamInfo.size.totalBytes;
      
      // مسار الحفظ في الموبايل
      final directory = await getApplicationDocumentsDirectory();
      String sanitizedTitle = title.replaceAll(RegExp(r'[^\w\s]+'), '');
      final filePath = '${directory.path}/$sanitizedTitle.mp4';
      final file = File(filePath);

      // عدد الأجزاء المطلوبة (32 جزءاً لسرعة خارقة)
      const int numParts = 32;
      int chunkSize = (totalSize / numParts).ceil();

      List<Future<void>> tasks = [];
      List<File> partFiles = [];

      int downloadedBytes = 0;
      final stopwatch = Stopwatch()..start();

      // 2. توزيع التحميل على 32 جزءاً متوازياً
      for (int i = 0; i < numParts; i++) {
        int start = i * chunkSize;
        int end = (i == numParts - 1) ? totalSize - 1 : (start + chunkSize - 1);
        
        if (start >= totalSize) break;

        String partPath = '${directory.path}/part_$i.tmp';
        partFiles.add(File(partPath));

        tasks.add(_downloadChunk(fileUrl, start, end, partPath, (int bytes) {
          downloadedBytes += bytes;
          double progress = downloadedBytes / totalSize;
          
          double elapsedSeconds = stopwatch.elapsedMilliseconds / 1000.0;
          if (elapsedSeconds > 0) {
            double speedBps = downloadedBytes / elapsedSeconds;
            String speedStr = _formatBytes(speedBps.toInt());
            onProgress(progress, '$speedStr/s');
          }
        }));
      }

      await Future.wait(tasks);

      // 3. دمج الأجزاء في ملف نهائي واحد
      IOSink sink = file.openWrite(mode: FileMode.writeOnly);
      for (var partFile in partFiles) {
        if (await partFile.exists()) {
          await sink.addStream(partFile.openRead());
          await partFile.delete();
        }
      }
      await sink.close();
      stopwatch.stop();

      onComplete();
    } catch (e) {
      print('خطأ أثناء التحميل: $e');
    } finally {
      yt.close();
    }
  }

  static Future<void> _downloadChunk(
    String url,
    int start,
    int end,
    String savePath,
    Function(int) onBytesDownloaded,
  ) async {
    final request = http.Request('GET', Uri.parse(url));
    request.headers['Range'] = 'bytes=$start-$end';

    final response = await http.Client().send(request);
    final file = File(savePath);
    final sink = file.openWrite();

    await response.stream.listen((chunk) {
      sink.add(chunk);
      onBytesDownloaded(chunk.length);
    }).asFuture();

    await sink.close();
  }

  static String _formatBytes(int bytes) {
    if (bytes < 1024) return '$bytes B';
    if (bytes < 1024 * 1024) return '${(bytes / 1024).toStringAsFixed(1)} KB';
    return '${(bytes / (1024 * 1024)).toStringAsFixed(1)} MB';
  }
}