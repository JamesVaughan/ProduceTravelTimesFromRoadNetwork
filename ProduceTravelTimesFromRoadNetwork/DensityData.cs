using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace ProduceTravelTimesFromRoadNetwork
{
    struct DensityData
    {
        public readonly float PopulationDensity;
        public readonly float EmploymentDensity;
        public readonly float HouseholdDensity;

        private DensityData(float populationDensity, float employmentDensity, float householdDensity)
        {
            PopulationDensity = populationDensity;
            EmploymentDensity = employmentDensity;
            HouseholdDensity = householdDensity;
        }

        public override bool Equals(object obj)
        {
            return obj is DensityData data &&
                   PopulationDensity == data.PopulationDensity &&
                   EmploymentDensity == data.EmploymentDensity &&
                   HouseholdDensity == data.HouseholdDensity;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(PopulationDensity, EmploymentDensity, HouseholdDensity);
        }

        public static Dictionary<int, DensityData> LoadDensityData(string filePath)
        {
            using (var reader = new StreamReader(filePath))
            {
                var ret = new Dictionary<int, DensityData>();
                string line;
                // burn header
                line = reader.ReadLine();
                while((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(',', StringSplitOptions.None);
                    if(parts.Length >= 8)
                    {
                        ret[int.Parse(parts[0])] = new DensityData(float.Parse(parts[5]), float.Parse(parts[6]), float.Parse(parts[7]));
                    }
                }
                return ret;
            }
        }
    }
}
