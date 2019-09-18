using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Text;

namespace MtcnnNet
{
    public class PeopleModel
    {
        public PeopleModel()
        {
            Photos = new List<string>();
            FreindIds = new List<string>();
        }
        [BsonId]
        public long VkId { get; set; }
        public string UserName { get; set; }
        public string UserCity { get; set; }
        public string UserLogo    { get; set; }
        public List<string>  Photos{get;set;}
        public List<string> FreindIds { get; set; }
        public string Birthday { get; set; }
    }
}
