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
        internal float TripStartTime;
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
            using (var reader = new CsvReader(filePath))
            {
                // burn header
                reader.LoadLine();
                long currentPerson = 0;
                bool validPerson = false;
                var seperators = new char[] { ',', '\t' };
                var toReturn = new SurveyEntry();
                while (reader.LoadLine(out var columns))
                {
                    if (columns > 19)
                    {
                        TripEntry trip;
                        // the last two digits are the ID for the trip
                        reader.Get(out long personNumber, 19);
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
                        if (!ConvertFloatStringToInt(reader, 6, out trip.Origin)
                            || !ConvertFloatStringToInt(reader, 7, out trip.Destination)
                            || !ConvertMode(reader, 9, out trip.Mode) || !ConvertTime(reader, 4, out trip.TripStartTime) || !ConvertTime(reader, 5, out trip.TripEndTime)
                            || !ConvertFloatStringToInt(reader, 22, out var originInZone)
                            || !ConvertFloatStringToInt(reader, 23, out var destinationInZone)
                            || originInZone < 1f
                            || destinationInZone < 1f)
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

        public static IEnumerable<SurveyEntry> EnumerateCellTraces(Network am, string stepFilePath)
        {
            Random r = new Random();
            using (var reader = new CsvReader(stepFilePath))
            {
                // burn header
                reader.LoadLine();
                string previousPersonID = null;
                string previousDate = null;
                bool validPerson = false;
                var toReturn = new SurveyEntry();
                while (reader.LoadLine(out var columns))
                {
                    if (columns >= 7)
                    {
                        reader.Get(out string stepType, 7);
                        reader.Get(out string date, 0);
                        if (stepType == "trip")
                        {
                            reader.Get(out string personID, 1);
                            // if we are starting a new person
                            if (previousPersonID != personID)
                            {
                                if (validPerson)
                                {
                                    yield return toReturn;
                                    validPerson = true;
                                    toReturn = new SurveyEntry();
                                }
                                else
                                {
                                    validPerson = true;
                                    toReturn.Trips.Clear();
                                }
                                previousDate = date;
                                previousPersonID = personID;
                            }
                            // only sore the data if they are valid
                            if (validPerson)
                            {
                                reader.Get(out int origin, 3);
                                reader.Get(out int destination, 4);
                                reader.Get(out float tripStart, 5);
                                reader.Get(out float tripEnd, 6);
                                if (!am.PickCentroidInZone(ref origin, r)
                                    || !am.PickCentroidInZone(ref destination, r))
                                {
                                    validPerson = false;
                                    continue;
                                }
                                toReturn.Add(new TripEntry()
                                {
                                    Mode = 0,
                                    TripStartTime = tripStart,
                                    TripEndTime = tripEnd,
                                    Origin = origin,
                                    Destination = destination
                                });
                            }
                        }
                    }
                }
                if (validPerson)
                {
                    yield return toReturn;
                }
            }
        }

        private static bool ConvertFloatStringToInt(CsvReader reader, int column, out int value)
        {
            reader.Get(out string toConvert, column);
            if (!float.TryParse(toConvert, out var temp))
            {
                value = 0;
                return false;
            }
            value = (int)temp;
            return true;
        }

        private static bool ConvertMode(CsvReader reader, int column, out byte mode)
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
            reader.Get(out string modeString, column);
            if (String.IsNullOrWhiteSpace(modeString))
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

        private static bool ConvertTime(CsvReader reader, int column, out float tripStart)
        {
            reader.Get(out string timeString, column);
            if (String.IsNullOrWhiteSpace(timeString))
            {
                tripStart = -1;
                return false;
            }
            if (!float.TryParse(timeString, out tripStart))
            {
                return false;
            }
            var hour = (int)tripStart;
            var ret = (hour * 60f) + (tripStart - hour) * 100f;
            tripStart = ret;
            return true;
        }
    }
}
