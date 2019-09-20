using Metrics;
using MongoDB.Driver;
using Python.Runtime;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MtcnnNet
{
    class Program
    {
        static dynamic cv2;
        static dynamic detector;
        static dynamic np;
        static dynamic code;
        private static IMongoCollection<PhotoModel> _collectionPhoto;
        private static IMongoCollection<PeopleModel> _collectionPeoples;
        private static IMongoCollection<ProcessingStateModel> _totalPeopleProcessed;
        private static ConcurrentQueue<string> queuePhotoToDownload = new ConcurrentQueue<string>();
        private static ConcurrentQueue<(string,string)> queuePhotoToPricessing = new ConcurrentQueue<(string,string)>();
        private static ConcurrentQueue<PhotoModel> queueResultToDbSave = new ConcurrentQueue<PhotoModel>();
        private static ConcurrentQueue<string> queueFileFaceImgProcessing = new ConcurrentQueue<string>();

        private static readonly Metrics.Timer timer = Metric.Timer("Photo processing Time", Unit.Requests);
        private static readonly Counter counter = Metric.Counter("Total photo processed", Unit.Requests);
        private static readonly Counter CurrentDownloadingTasks= Metric.Counter("CurrentDownloadingTasks", Unit.Requests);
        private static readonly Counter TotalPeopleProcessed = Metric.Counter("TotalPeopleProcessed", Unit.Items);
       

        private static readonly string FaceImgFolderBasePath = @"../../../../facedb";
        private static readonly string ColabGoogleDrivePath = @"/content/gdrive/My drive/facedb";

        static void Main(string[] args)
        {

            Metric.Gauge("FileToTransferCount",()=> { return queueFileFaceImgProcessing.Count; }, Unit.Items);
            Metric.Config

                //   // .WithHttpEndpoint("http://+:1234/")
                .WithReporting(report => report.WithReport(new ConsoleMetricReporter(), TimeSpan.FromSeconds(300)));

               // .WithAppCounters();
            //    .WithAllCounters();


            Console.WriteLine("Start db connection");

            string connectionString = "mongodb://79.143.30.220:27088";
            var client = new MongoClient(connectionString);
            var _db = client.GetDatabase("vk");
            _collectionPhoto = _db.GetCollection<PhotoModel>("photos");
            _collectionPeoples = _db.GetCollection<PeopleModel>("peoples");
            _totalPeopleProcessed = _db.GetCollection<ProcessingStateModel>("ProcessingState");

            Console.WriteLine("Db connection OK");


            

            var q1 = Builders<PeopleModel>.Filter.Regex(x => x.UserCity, new MongoDB.Bson.BsonRegularExpression("Орен"));
            var q2 = Builders<PeopleModel>.Filter.Eq(x => x.Photos, null);
            var q3 = Builders<PeopleModel>.Filter.Not(q2);
            var q = Builders<PeopleModel>.Filter.And(q1, q3);

            var totalPeopleProcessing = 67000;
            var sessionPeopleProcessing = 0;
            var qq2 = Builders<ProcessingStateModel>.Filter.Eq(x => x.Id, 1);
            totalPeopleProcessing =  _totalPeopleProcessed.Find(qq2).ToList().FirstOrDefault().PeopleProcessing;

            Console.WriteLine("Start peopele to processing ressived");
            var peopleToProcessingCursor = _collectionPeoples.Find(q, new FindOptions { NoCursorTimeout = true }).Skip(totalPeopleProcessing);
            var peopleToProcessing = peopleToProcessingCursor.ToEnumerable();
           // Console.WriteLine("People to processing: "+peopleToProcessing.Count);


            // PythonEngine.BeginAllowThreads();
            Console.WriteLine("Start thread init");
            var threatPhotoDownload = new Thread(DownloadTask);
            var threadPhotoProcessing = new Thread(ProcessPhotoTask);
            var threadDbSaveProcessing = new Thread(SaveToDbTask);
            var threadFileFaceImgProcessing = new Thread(FileFaceImgProcessingTask);
            threadPhotoProcessing.Start();
            threatPhotoDownload.Start();
            threadDbSaveProcessing.Start();
            threadFileFaceImgProcessing.Start();

            Console.WriteLine("Thread init OK");

            foreach (var people in peopleToProcessing)
            {

                TotalPeopleProcessed.Increment();
                totalPeopleProcessing++;
                sessionPeopleProcessing++;
                if (people.Photos == null) continue;
                foreach (var photo in people.Photos)
                {
                    if (string.IsNullOrWhiteSpace(photo)) continue;
                    if (photo == "https://vk.com/images/x_null.gif") continue;
                    if (_collectionPhoto.Find(Builders<PhotoModel>.Filter.Eq(x => x.photo, photo)).Limit(1).CountDocuments() == 0)
                    {
                          queuePhotoToDownload.Enqueue(photo);
                        if (queuePhotoToDownload.Count > 120)
                        {
                            while (queuePhotoToDownload.Count > 100)
                            {
                                Thread.Sleep(100);
                            }
                        }
                    }
                }
                if(totalPeopleProcessing % 300 == 0)
                {
                    var qqq = Builders<ProcessingStateModel>.Filter.Eq(x => x.Id, 1);
                    var updatum = Builders<ProcessingStateModel>.Update.Set(x => x.PeopleProcessing, totalPeopleProcessing);
                    _totalPeopleProcessed.UpdateOne(qqq, updatum);
                }
            }


        }

        private static void ProcessPhotoTask()
        {   
            Console.WriteLine(PythonEngine.Version);
            using (Py.GIL())
            {
                PythonEngine.Exec(@"import sys
sys.path.insert(0, '/content/MtcnnNet/')");
                np = Py.Import("numpy");
                code = Py.Import("codeMy");
                dynamic mtcnn = Py.Import("mtcnn.mtcnn");
                detector = mtcnn.MTCNN(min_face_size: 25);
                cv2 = Py.Import("cv2");

                while(true)
                {
                    if(queuePhotoToPricessing.Count > 0)
                    {
                        (string, string) state;
                        if (queuePhotoToPricessing.TryDequeue(out state))
                        {
                            ProcessUrl(state.Item2, state.Item1);
                        }
                    }
                    else
                    {
                        Thread.Sleep(10);
                    }
                }

            }
        }

        public static string CreateMD5(string input)
        {
            // Use input string to calculate MD5 hash
            using (System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create())
            {
                byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
                byte[] hashBytes = md5.ComputeHash(inputBytes);

                // Convert the byte array to hexadecimal string
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < hashBytes.Length; i++)
                {
                    sb.Append(hashBytes[i].ToString("x2"));
                }
                return sb.ToString();
            }
        }

        static void ProcessUrl(string url, string filename)
        {
            try
            {
                using (timer.NewContext()) // measure until disposed
                {
                    var img = cv2.imread(filename);
                    if (img != null)
                    {
                        // img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB);

                        int srcImageWidth = img.shape[0].As<int>();
                        int srcImageHeight = img.shape[1].As<int>();
                        var minSrcImageSize = Math.Min(srcImageHeight, srcImageWidth);
                        var scalefactor = 200f / minSrcImageSize;
                        if (scalefactor > 1) scalefactor = 1;
                        else if (scalefactor > 0.3) scalefactor = 0.3f;
                        var newWidth = new PyInt((int)(srcImageWidth * scalefactor));
                        var newHeight = new PyInt((int)(srcImageHeight * scalefactor));

                        var ffff = new PyTuple(new[] { newHeight, newWidth });

                        var imgToFaceDetection = cv2.resize(img, dsize: ffff);
                        var newShape = imgToFaceDetection.shape;

                        var faces = detector.detect_faces(imgToFaceDetection);
                        var facesTest = detector.detect_faces(img);

                        var photoModel = new PhotoModel();
                        photoModel.photo = url;


                        var fountFacesCount = (faces as PyObject).Length();
                        for (var ee = 0; ee < fountFacesCount; ee++)
                        {
                            var confidence = faces[ee]["confidence"].As<float>();
                            if (confidence < 0.98) continue;

                            var LeftEKeyPoint = np.array((faces[ee]["keypoints"]["left_eye"]));
                            var RightEKeyPoint = np.array(faces[ee]["keypoints"]["right_eye"]);

                            var LeftMKeyPoint = np.array(faces[ee]["keypoints"]["mouth_left"]);
                            var RightMKeyPoint = np.array(faces[ee]["keypoints"]["mouth_right"]);

                            var NoseKeyPoint = np.array(faces[ee]["keypoints"]["nose"]);

                            var outKeyPointsPyList = np.array(new List<dynamic> { LeftEKeyPoint, RightEKeyPoint, NoseKeyPoint, LeftMKeyPoint, RightMKeyPoint }) / scalefactor;
                            // var outBBox = new List<int> { boxTop, boxLeft, boxTop + boxW, boxLeft + boxH };


                            var fff = code.preprocess(img, landmark: outKeyPointsPyList);

                            var filenameOut = CreateMD5(url) + '_' + ee.ToString();
                            var folder1 = filenameOut[0];
                            var folder2 = filenameOut[1];
                            var folder3 = filenameOut[2];

                            var saveFolderName = Path.GetDirectoryName($"{FaceImgFolderBasePath}/{folder1}/{folder2}/{folder3}/");
                            if (!Directory.Exists(saveFolderName))
                            {
                                Directory.CreateDirectory(saveFolderName);
                            }
                            var saveFilename = Path.Join(saveFolderName, filenameOut + ".jpg");
                            cv2.imwrite(saveFilename, fff);
                            queueFileFaceImgProcessing.Enqueue(saveFilename);
                            var boxum = ((int[])faces[ee]["box"].As<int[]>()).ToList();
                            photoModel.faces.Add(new Face { box = boxum, filename = filenameOut });
                        }

                        //_collectionPhoto.InsertOne(photoModel);
                        queueResultToDbSave.Enqueue(photoModel);
                        counter.Increment();
                    }
                }
            }
            catch(Exception ex)
            {

            }
            finally
            {
                try
                {
                    File.Delete(filename);
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                
            }
            
        }

        static int DownloadedCount = 0;
        static void DownloadTask()
        {
            while (true)
            {
                if (queuePhotoToDownload.Count > 0)
                {
                    while(queuePhotoToPricessing.Count>100 || DownloadedCount>10)
                    {
                        Thread.Sleep(100);
                    }
                    var url = "";
                    if (queuePhotoToDownload.TryDequeue(out url))
                    {
                        var client = new HttpClient();
                        DownloadedCount++;
                        CurrentDownloadingTasks.Increment();
                        client.GetByteArrayAsync(url).ContinueWith(ProcessDownloaded,url);
                    }
                }
            }
        }

        static void FileFaceImgProcessingTask()
        {
            while(true)
            {
                while (queueFileFaceImgProcessing.Count > 0)
                {
                    if (queueFileFaceImgProcessing.TryDequeue(out string fullFilename))
                    {
                        try
                        {
                            var filename = Path.GetFileName(fullFilename);
                            var newFolderName = $"{ColabGoogleDrivePath}/{filename[0]}/{filename[1]}/{filename[2]}";
                            if(!Directory.Exists(newFolderName))
                            {
                                Directory.CreateDirectory(newFolderName);
                            }
                            var newFilename = Path.Combine(newFolderName, filename);

                            File.Copy(fullFilename, newFilename);
                            File.Delete(fullFilename);
                            
                        }
                        catch
                        {

                        }
                    }
                }
            }
        }


        static void SaveToDbTask()
        {
            while(true)
            {
                if(queueResultToDbSave.Count>50)
                {
                    var buffer = new List<PhotoModel>();
                    for(var h=0;h<50;h++)
                    {
                        if(queueResultToDbSave.TryDequeue(out PhotoModel photoModel))
                        {
                            buffer.Add(photoModel);
                        }
                    }
                    _collectionPhoto.InsertMany(buffer);
                }
                else
                {
                    Thread.Sleep(100);
                }
            }
        }

        private static void ProcessDownloaded(Task<byte[]> arg1, object state)
        {
            DownloadedCount--;
            CurrentDownloadingTasks.Decrement();
            if (arg1.IsCompleted)
            {
                try
                {
                    var downloadedPhoto = arg1.Result;
                    var tmpFilename = ((string)state).Replace("http://", "").Replace("https://", "").Replace("/", "_");
                    File.WriteAllBytes(tmpFilename, downloadedPhoto);
                    queuePhotoToPricessing.Enqueue((tmpFilename, (string)state));
                }
                catch
                {

                }
            }

        }
    }
}
