import 'package:flutter/material.dart';
import 'dart:async';
import 'dart:io';
import 'package:http/http.dart' as http;
void main() {
  // هنا تبدأ تشغيل التطبيق أو الواجهة الخاصة بك
  runApp(const MaterialApp(
    home: Scaffold(
      body: Center(child: Text('Hammer Downloader')),
    ),
  ));
}
/// نموذج يمثل حالة وحجم التحميل
class DownloadProgress {
  final int downloadedBytes;
  final int totalBytes;
  final double progressPercentage;
  final bool isCompleted;
  final String? errorMessage;

  DownloadProgress({
    required this.downloadedBytes,
    required this.totalBytes,
    required this.progressPercentage,
    this.isCompleted = false,
    this.errorMessage,
  });
}

/// محرك التحميل المتعدد الأجزاء لـ Hammer Downloader
class DownloadEngine {
  final int connectionsCount; // عدد الأجزاء/وصلات التحميل (مثل 32)
  bool _isCancelled = false;

  DownloadEngine({this.connectionsCount = 32});

  /// ألغاء عملية التحميل
  void cancelDownload() {
    _isCancelled = true;
  }

  /// دالة بدء تحميل ملف وتقسيمه
  Stream<DownloadProgress> startDownload({
    required String fileUrl,
    required String savePath,
  }) async* {
    _isCancelled = false;

    try {
      // 1. الحصول على حجم الملف الإجمالي من السيرفر
      final headResponse = await http.head(Uri.parse(fileUrl));
      final contentLengthHeader = headResponse.headers['content-length'];

      if (contentLengthHeader == null) {
        yield DownloadProgress(
          downloadedBytes: 0,
          totalBytes: 0,
          progressPercentage: 0,
          errorMessage: 'تعذر تحديد حجم الملف من السيرفر.',
        );
        return;
      }

      final int totalBytes = int.parse(contentLengthHeader);
      final int chunkSize = (totalBytes / connectionsCount).ceil();

      List<int> downloadedChunkBytes = List.filled(connectionsCount, 0);
      final File targetFile = File(savePath);
      final RandomAccessFile openFile = await targetFile.open(mode: FileMode.write);

      // تجهيز حجم الملف مبدئياً على القرص الصلب
      await openFile.truncate(totalBytes);
      await openFile.close();

      // 2. إنشاء مصفوفة المهام للتحميل المتوازي (Parallel Downloading)
      final controller = StreamController<DownloadProgress>();

      int overallDownloaded = 0;

      // بدء تحميل كل جزء بشكل منفصل
      List<Future<void>> downloadTasks = [];

      for (int i = 0; i < connectionsCount; i++) {
        final int startByte = i * chunkSize;
        final int endByte = (i == connectionsCount - 1) ? totalBytes - 1 : (startByte + chunkSize - 1);

        downloadTasks.add(
          _downloadChunk(
            fileUrl: fileUrl,
            savePath: savePath,
            chunkIndex: i,
            startByte: startByte,
            endByte: endByte,
            onProgress: (bytesRead) {
              if (_isCancelled) return;
              downloadedChunkBytes[i] += bytesRead;
              overallDownloaded = downloadedChunkBytes.reduce((a, b) => a + b);

              double progressPercentage = (overallDownloaded / totalBytes) * 100;
              controller.add(
                DownloadProgress(
                  downloadedBytes: overallDownloaded,
                  totalBytes: totalBytes,
                  progressPercentage: progressPercentage,
                ),
              );
            },
          ),
        );
      }

      // انتظام المهام وإرسال التحديثات للمستخدم
      Future.wait(downloadTasks).then((_) {
        if (!_isCancelled) {
          controller.add(
            DownloadProgress(
              downloadedBytes: totalBytes,
              totalBytes: totalBytes,
              progressPercentage: 100.0,
              isCompleted: true,
            ),
          );
        }
        controller.close();
      }).catchError((error) {
        controller.add(
          DownloadProgress(
            downloadedBytes: overallDownloaded,
            totalBytes: totalBytes,
            progressPercentage: 0,
            errorMessage: 'حدث خطأ أثناء التحميل: $error',
          ),
        );
        controller.close();
      });

      yield* controller.stream;
    } catch (e) {
      yield DownloadProgress(
        downloadedBytes: 0,
        totalBytes: 0,
        progressPercentage: 0,
        errorMessage: 'خطأ في الاتصال: $e',
      );
    }
  }

  /// تحميل جزء محدد (Chunk) وكتابته في موقعه الصحيح داخل الملف
  Future<void> _downloadChunk({
    required String fileUrl,
    required String savePath,
    required int chunkIndex,
    required int startByte,
    required int endByte,
    required Function(int) onProgress,
  }) async {
    final request = http.Request('GET', Uri.parse(fileUrl));
    request.headers['Range'] = 'bytes=$startByte-$endByte';

    final response = await http.Client().send(request);

    if (response.statusCode == 206 || response.statusCode == 200) {
      final File targetFile = File(savePath);
      final RandomAccessFile openFile = await targetFile.open(mode: FileMode.append);

      // الانتقال إلى بداية مكان الجزء المحدد في الملف
      await openFile.setPosition(startByte);

      await for (var chunk in response.stream) {
        if (_isCancelled) break;
        await openFile.writeFrom(chunk);
        onProgress(chunk.length);
      }

      await openFile.close();
    } else {
      throw Exception('فشل السيرفر في قبول استجابة Range للجزء $chunkIndex');
    }
  }
}
