using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Text;

namespace MtcnnNet
{
    public class ProcessingStateModel
    {
        [BsonId]
        public int Id { get; set; } = 1;
        public int PeopleProcessing { get; set; }
    }
}
