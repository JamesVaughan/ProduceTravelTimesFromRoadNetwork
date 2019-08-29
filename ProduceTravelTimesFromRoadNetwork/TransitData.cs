using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace ProduceTravelTimesFromRoadNetwork
{
    /// <summary>
    /// Contains a record of a transit trip
    /// </summary>
    sealed class TransitData
    {
        /// <summary>
        /// The origin node
        /// </summary>
        public readonly int OriginNode;

        /// <summary>
        /// The destination node
        /// </summary>
        public readonly int DestinationNode;

        /// <summary>
        /// Time of day in minutes from midnight
        /// </summary>
        public readonly int TimeOfDay;

        private TransitData(int originNode, int destinationNode, string timeString)
        {
            OriginNode = originNode;
            DestinationNode = destinationNode;
            TimeOfDay = ConvertTimeToMinutesFromMidnight(timeString);
        }

        private static int ConvertTimeToMinutesFromMidnight(string timeString)
        {
            // Example: 2018-05-02  01:11:15
            int i = 0;
            int count = 0;
            for (; i < timeString.Length && count < 2; i++)
            {
                if(timeString[i] == '-')
                {
                    count++;
                }
            }
            // find the next space
            for (; i < timeString.Length && timeString[i] == ' '; i++) { }
            int ret = 0;
            // Read the hours
            for (count = 0; i < timeString.Length && timeString[i] == ':'; i++, count++) { }
            if (i < timeString.Length) ret = int.Parse(timeString.AsSpan(i - count, count)) * 60;
            // Read the minutes
            for (count = 0; i < timeString.Length && timeString[i] == ':'; i++, count++) { }
            if (i < timeString.Length) ret += int.Parse(timeString.AsSpan(i - count, count));
            // Ignore seconds
            return ret;
        }

        public static IEnumerator<TransitData> StreamTransitData(string surveyPath, Dictionary<int, int> stopToNode)
        {
            // convert survey data
            using(var reader = new StreamReader(surveyPath))
            {
                // burn header
                reader.ReadLine();
                string line;
                while((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(',');
                    // if we have a valid line
                    if(parts.Length >= 29 && parts[2] == "e-payment")
                    {
                        yield return new TransitData(
                            stopToNode[int.Parse(parts[6])],
                            stopToNode[int.Parse(parts[13])],
                            parts[5]);
                    }
                }
            }
        }

        public static IEnumerator<List<TransitData>> StreamTransitRiderDays(string surveyPath, Dictionary<int, int> stopToNode)
        {
            /*int previousUser = -1;
            bool any = false;
            var ret = new 
            */
            return null;
        }

    }
}
