using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace ProduceTravelTimesFromRoadNetwork
{

    struct TripEntry
    {
        internal int Origin;
        internal int Destination;
        /// <summary>
        /// In Minutes from Midnight
        /// </summary>
        internal float TripStart;
        /// <summary>
        /// In Minutes from Midnight
        /// </summary>
        internal float TripEndTime;
        /// <summary>
        /// 0 = auto, 1 = transit, 2 = active
        /// </summary>
        internal byte Mode;
    }

    sealed class SurveyEntry
    {
        public readonly List<TripEntry> Trips = new List<TripEntry>(10);

        internal void Add(TripEntry entry)
        {
            Trips.Add(entry);
        }

        internal void Clear()
        {
            Trips.Clear();
        }
    }

    static class Survey
    {
        public static IEnumerable<SurveyEntry> EnumerateSurvey(string filePath)
        {
            using (var reader = new StreamReader(filePath))
            {
                // burn header
                reader.ReadLine();
                string line;
                long currentPerson = 0;
                bool validPerson = false;
                var seperators = new char[] { ',', '\t' };
                var toReturn = new SurveyEntry();
                while ((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(seperators);
                    if (parts.Length > 19)
                    {
                        TripEntry trip;
                        // the last two digits are the ID for the trip
                        long personNumber;
                        if(!long.TryParse(parts[19], out personNumber))
                        {
                            personNumber = 0;
                            continue;
                        }
                        personNumber = personNumber / 100;
                        if (personNumber != currentPerson)
                        {
                            if (validPerson)
                            {
                                yield return toReturn;
                                toReturn = new SurveyEntry();
                            }
                            else
                            {
                                toReturn.Clear();
                            }
                            currentPerson = personNumber;
                            validPerson = true;
                        }
                        if (!ConvertFloatStringToInt(parts[6], out trip.Origin)
                            || !ConvertFloatStringToInt(parts[7], out trip.Destination)
                            || !ConvertMode(parts[9], out trip.Mode) || !ConvertTime(parts[4], out trip.TripStart) || !ConvertTime(parts[5], out trip.TripEndTime))

                        {
                            validPerson = false;
                        }
                        else
                        {
                            toReturn.Add(trip);
                        }
                    }
                }
                if (validPerson)
                {
                    yield return toReturn;
                }
            }
        }

        private static bool ConvertFloatStringToInt(string toConvert, out int value)
        {
            if(!float.TryParse(toConvert, out var temp))
            {
                value = 0;
                return false;
            }
            value = (int)temp;
            return true;
        }

        private static bool ConvertMode(string modeString, out byte mode)
        {
            mode = 0;
            /*
                0 "Otros, sin especificar" = Other / Not Specified
                1 "A pie" = On Foot
                2 "Bicicleta" = Bicycle
                3 "Otro, animal" = Animal
                4 "Remise" = 'Discount'
                5 "Otro Uber" = 'Another Uber'
                6 "Taxi" = 'Taxi'
                7 "Auto pasajero" = 'Auto Passenger'
                8 "Auto conductor" = 'Auto Driver'
                9 "Moto pasajero" = 'Motorcycle Passenger'
                10 "Moto conductor" = 'Motorcycle Driver'
                11 "Bus escolar" = 'School Bus'
                12 "Bus de la empresa" = 'Company Bus'
                13 "Bus" = 'Bus'
                14 "Ferrocarril" = 'Railway'
             */
            if(String.IsNullOrWhiteSpace(modeString))
            {
                return false;
            }
            switch ((int)float.Parse(modeString))
            {
                case 4:
                case 5:
                case 6:
                case 7:
                case 8:
                case 9:
                case 10:
                case 11: // not sure about schoolbus being auto
                case 12:
                    mode = 0;
                    break;
                case 13:
                case 14:
                    mode = 1;
                    break;
                case 1:
                case 2:
                case 3:
                    mode = 2;
                    break;
                case 0:
                    return false;
                default:
                    return false;
            }
            return true;
        }

        private static bool ConvertTime(string timeString, out float tripStart)
        {
            if (String.IsNullOrWhiteSpace(timeString))
            {
                tripStart = -1;
                return false;
            }
            tripStart = float.Parse(timeString) / 60f;
            return true;
        }
    }
}
