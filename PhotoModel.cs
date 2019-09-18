using System;
using System.Collections.Generic;
using System.Text;

namespace MtcnnNet
{
    public class Face
    {
        public List<int> box { get; set; }
        public string filename { get; set; }
    }

    public class PhotoModel
    {
        public string photo { get; set; }
        public List<Face> faces { get; set; } = new List<Face>();
    }

}
