using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GeoAPI.Geometries;
using NetTopologySuite;
using NetTopologySuite.IO;

namespace ProduceTravelTimesFromRoadNetwork
{

    struct Node
    {
        public int NodeNumber;
        public float X;
        public float Y;
        public int ZoneNumber;
        public bool Centroid;
        public List<int> LinksTo;
    }

    struct Link
    {
        public readonly int I, J;
        public float GeneralCost;
        public float TravelTime;

        public Link(int i, int j)
        {
            I = i;
            J = j;
            GeneralCost = 0;
            TravelTime = 0;
        }
    }

    struct TurnRestriction
    {
        public readonly int I, J, K;

        public TurnRestriction(int i, int j, int k)
        {
            I = i;
            J = j;
            K = k;
        }
    }

    sealed class Network
    {
        private readonly Dictionary<int, Node> _nodes = new Dictionary<int, Node>();
        private readonly Dictionary<(int, int), Link> _links = new Dictionary<(int, int), Link>();
        private readonly HashSet<(int i, int j, int k)> _turnRestrictions = new HashSet<(int i, int j, int k)>();

        private Network()
        {

        }

        private void Add(Node node)
        {
            node.LinksTo = node.LinksTo ?? new List<int>();
            _nodes[node.NodeNumber] = node;
        }

        private void Add(Link link)
        {
            _nodes[link.I].LinksTo.Add(link.J);
            _links[(link.I, link.J)] = link;
        }

        private void Add(TurnRestriction turn)
        {
            _turnRestrictions.Add((turn.I, turn.J, turn.K));
        }

        public static Network LoadNetwork(string nwpPath, string shapeFile, string zoneFieldName)
        {
            if (string.IsNullOrWhiteSpace(nwpPath))
            {
                throw new ArgumentException("We need to know the path for the network package.", nameof(nwpPath));
            }

            if (string.IsNullOrWhiteSpace(shapeFile))
            {
                throw new ArgumentException("We need to know the path to the shape file.", nameof(shapeFile));
            }

            if (string.IsNullOrWhiteSpace(zoneFieldName))
            {
                throw new ArgumentException("We need to know the name of the field to load the zones numbers from.", nameof(zoneFieldName));
            }
            // Load the shape file
            return LoadNWP(nwpPath, LoadZones(shapeFile, zoneFieldName));
        }

        private static Network LoadNWP(string NWPLocation, List<(int zoneNumber, IGeometry geometry)> zones)
        {
            if (!File.Exists(NWPLocation))
            {
                return null;
            }
            Network network = new Network();
            using (ZipArchive archive = new ZipArchive(File.OpenRead(NWPLocation), ZipArchiveMode.Read, false))
            {
                ZipArchiveEntry nodes = null, links = null, exAtt = null, turns = null;
                foreach (var entry in archive.Entries)
                {
                    if (entry.Name.Equals("base.211", StringComparison.InvariantCultureIgnoreCase))
                    {
                        nodes = entry;
                    }
                    else if (entry.Name.Equals("link_results.csv", StringComparison.InvariantCultureIgnoreCase))
                    {
                        links = entry;
                    }
                    else if (entry.Name.Equals("exatt_links.241", StringComparison.InvariantCultureIgnoreCase))
                    {
                        exAtt = entry;
                    }
                    else if(entry.Name.Equals("turns.231", StringComparison.InvariantCultureIgnoreCase))
                    {
                        turns = entry;
                    }
                }
                LoadNodes(nodes, network, zones);
                LoadLinks(links, exAtt, network);
                LoadTurns(turns, network);
                return network;
            }
        }

        private static void LoadNodes(ZipArchiveEntry nodeArchive, Network network, List<(int, IGeometry)> zones)
        {
            using (var reader = new StreamReader(nodeArchive.Open()))
            {
                string line = null;
                const string nodesMarker = "t nodes";
                while ((line = reader.ReadLine()) != null && line != nodesMarker) ;
                if (line != nodesMarker)
                {
                    return;
                }
                // burn the header
                reader.ReadLine();
                var seperators = new char[] { ' ', '\t' };
                while ((line = reader.ReadLine()) != null && line[0] != 't')
                {
                    // ignore blank lines
                    if (line.Length > 2)
                    {
                        // if it is a centroid
                        if (line[0] == 'a')
                        {
                            var split = line.Split(seperators, StringSplitOptions.RemoveEmptyEntries);
                            if (split.Length < 3)
                            {
                                continue;
                            }
                            if (!(int.TryParse(split[1], out int zoneNumber)
                                && float.TryParse(split[2], out float x)
                                && float.TryParse(split[3], out float y)))
                            {
                                return;
                            }
                            network.Add(new Node()
                            {
                                NodeNumber = zoneNumber,
                                X = x,
                                Y = y,
                                Centroid = (line[1] == '*'),
                                ZoneNumber = GetZoneNumber(x, y, zones)
                            });
                        }
                    }
                }
            }
        }

        private static void LoadLinks(ZipArchiveEntry linkArchive, ZipArchiveEntry exAttArchive, Network network)
        {
            using (var reader = new StreamReader(linkArchive.Open()))
            {
                // burn header
                string line = reader.ReadLine();
                while ((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(',');
                    if (parts.Length >= 5)
                    {
                        network.Add(new Link(int.Parse(parts[0]), int.Parse(parts[1]))
                        {
                            TravelTime = float.Parse(parts[4])
                        });
                    }
                }
            }
            using (var reader = new StreamReader(exAttArchive.Open()))
            {
                var line = reader.ReadLine();
                var parts = line.Split(',');
                while ((line = reader.ReadLine()) != null)
                {
                    parts = line.Split(',');
                    if (parts.Length >= 12)
                    {
                        int i = int.Parse(parts[0]);
                        int j = int.Parse(parts[1]);
                        var link = network._links[(i, j)];
                        link.GeneralCost = float.Parse(parts[12]);
                        // store the result back
                        network._links[(i, j)] = link;
                    }
                }
            }
        }

        private static void LoadTurns(ZipArchiveEntry turnsArchive, Network network)
        {
            using (var reader = new StreamReader(turnsArchive.Open()))
            {
                string line = null;
                const string nodesMarker = "t nodes";
                while ((line = reader.ReadLine()) != null && line != nodesMarker) ;
                if (line != nodesMarker)
                {
                    return;
                }
                // burn the header
                reader.ReadLine();
                var seperators = new char[] { ' ', '\t' };
                while ((line = reader.ReadLine()) != null && line[0] != 't')
                {
                    // ignore blank lines
                    if (line.Length > 2)
                    {
                        // if it is a centroid
                        if (line[0] == 'a')
                        {
                            var split = line.Split(seperators, StringSplitOptions.RemoveEmptyEntries);
                            if (split.Length > 4 && int.Parse(split[4]) == 0)
                            {
                                network.Add(new TurnRestriction(int.Parse(split[1]), int.Parse(split[2]), int.Parse(split[3])));
                            }
                        }
                    }
                }
            }
        }

        private static int GetZoneNumber(float x, float y, List<(int zoneNumber, IGeometry geometry)> zones)
        {
            var factory = NetTopologySuite.Geometries.GeometryFactory.Default;
            var point = factory.CreatePoint(new Coordinate(x, y));
            foreach (var (zoneNumber, geometry) in zones)
            {
                if (geometry.Contains(point))
                {
                    return zoneNumber;
                }
            }
            return -1;
        }

        private static List<(int zoneNumber, IGeometry geometry)> LoadZones(string shapeFile, string zoneFieldName)
        {
            var factory = NetTopologySuite.Geometries.GeometryFactory.Default;
            var zones = new List<(int, IGeometry)>();
            using (var dbReader = new NetTopologySuite.IO.ShapefileDataReader(shapeFile, factory))
            {
                var zoneIndexNumber = GetZoneIndex(dbReader, zoneFieldName);
                if (zoneIndexNumber < 0)
                {
                    return null;
                }
                while (dbReader.Read())
                {
                    var zoneNumber = dbReader.GetInt32(zoneIndexNumber);
                    if (zoneNumber > 0)
                    {
                        var geometry = dbReader.Geometry;
                        zones.Add((zoneNumber, geometry));
                    }
                }
            }
            return zones;
        }

        private static int GetZoneIndex(ShapefileDataReader dbReader, string zoneFieldName)
        {
            var fields = dbReader.DbaseHeader.Fields;
            for (int i = 0; i < fields.Length; i++)
            {
                if (fields[i].Name == zoneFieldName)
                {
                    return i + 1;
                }
            }
            return -1;
        }

        sealed class MinHeap
        {
            private List<((int origin, int destination) link, (int parentOrigin, int parentDestination) parentLink, float cost)> _data = new List<((int, int), (int, int), float)>();

            public int Count => _data.Count;

            public ((int origin, int destination) link, (int parentOrigin, int parentDestination) parentLink, float cost) PopMin()
            {
                var tailIndex = _data.Count - 1;
                if (tailIndex < 0)
                {
                    return ((-1, -1),(-1, -1),-1);
                }
                var top = _data[0];
                var last = _data[tailIndex];
                _data[0] = last;
                var current = 0;
                while (current < _data.Count)
                {
                    var childrenIndex = (current << 1) + 1;
                    if (childrenIndex + 1 < _data.Count)
                    {
                        if (_data[childrenIndex].cost < _data[current].cost)
                        {
                            _data[current] = _data[childrenIndex];
                            _data[childrenIndex] = last;
                            current = childrenIndex;
                            continue;
                        }
                        if (_data[childrenIndex + 1].cost < _data[current].cost)
                        {
                            _data[current] = _data[childrenIndex + 1];
                            _data[childrenIndex + 1] = last;
                            current = childrenIndex + 1;
                            continue;
                        }
                    }
                    else if (childrenIndex < _data.Count)
                    {
                        if (_data[childrenIndex].cost < _data[current].cost)
                        {
                            _data[current] = _data[childrenIndex];
                            _data[childrenIndex] = last;
                            current = childrenIndex;
                            continue;
                        }
                    }
                    break;
                }
                _data.RemoveAt(_data.Count - 1);
                return (top.link, top.parentLink, top.cost);
            }

            public void Push((int origin, int destination) link, (int parentOrigin, int parentDestination) parentLink, float cost)
            {
                int current = 0;
                for (; current < _data.Count; current++)
                {
                    if (_data[current].link == link)
                    {
                        // if we found a better path to this node
                        if (_data[current].cost > cost)
                        {
                            var temp = _data[current];
                            temp.parentLink = parentLink;
                            temp.cost = cost;
                            _data[current] = temp;
                            break;
                        }
                        else
                        {
                            // if the contained child is already better ignore the request
                            return;
                        }
                    }
                }
                if (current == _data.Count)
                {
                    // if it is not already contained
                    _data.Add((link, parentLink, cost));
                }
                // we don't need to check the root
                while (current >= 1)
                {
                    var parentIndex = current >> 1;
                    var parent = _data[parentIndex];
                    if (parent.cost <= _data[current].cost)
                    {
                        break;
                    }
                    _data[parentIndex] = _data[current];
                    _data[current] = parent;
                    current = parentIndex;
                }
            }
        }

        /// <summary>
        /// Thread-safe on a static network
        /// </summary>
        /// <param name="originZoneNumber"></param>
        /// <param name="destinationZoneNumber"></param>
        /// <returns></returns>
        public List<(int origin, int destination)> GetFastestPath(int originZoneNumber, int destinationZoneNumber)
        {
            var fastestParent = new Dictionary<(int origin, int destination), (int parentOrigin, int parentDestination)>();
            MinHeap toExplore = new MinHeap();
            foreach (var link in _nodes[originZoneNumber].LinksTo)
            {
                toExplore.Push((originZoneNumber, link), (-1, originZoneNumber), _links[(originZoneNumber, link)].GeneralCost);
            }
            while (toExplore.Count > 0)
            {
                var current = toExplore.PopMin();
                // don't explore things that we have already done
                if (fastestParent.ContainsKey(current.link))
                {
                    // check to see if there are some turns that were restricted that need to be explored
                    continue;
                }
                // check to see if we have hit our destination
                int currentDestination = current.link.destination;
                if (currentDestination == destinationZoneNumber)
                {
                    return GeneratePath(fastestParent, current);
                }
                fastestParent[current.link] = current.parentLink;
                var node = _nodes[currentDestination];
                var links = node.LinksTo;
                foreach (var childDestination in links)
                {
                    // explore everything that hasn't been solved, the min heap will update if it is a faster path to the child node
                    if (!fastestParent.ContainsKey((currentDestination, childDestination)))
                    {
                        // don't explore centroids that are not our destination
                        if(!_nodes[childDestination].Centroid || childDestination == destinationZoneNumber)
                        {
                            // ensure there is not a turn restriction
                            if (!_turnRestrictions.Contains((current.link.origin, currentDestination, childDestination)))
                            {
                                // make sure cars are allowed on the link
                                var linkCost = _links[(currentDestination, childDestination)].GeneralCost;
                                if (linkCost >= 0)
                                {
                                    toExplore.Push((currentDestination, childDestination), current.link, current.cost + linkCost);
                                }
                            }
                        }
                    }
                }
            }
            return null;
        }

        private static List<(int origin, int destination)> GeneratePath(Dictionary<(int origin, int destination), (int parentOrigin, int parentDestination)> fastestParent,
            ((int origin, int destination) link, (int parentOrigin, int parentDestination) parentLink, float cost) current)
        {
            // unwind the parents to build the path
            var ret = new List<(int, int)>();
            var cIndex = current.parentLink;
            ret.Add(current.link);
            if (cIndex.parentOrigin >= 0)
            {
                ret.Add(cIndex);
                while (true)
                {
                    if (fastestParent.TryGetValue(cIndex, out var parent))
                    {
                        if (parent.parentOrigin >= 0)
                        {
                            ret.Add((cIndex = parent));
                            continue;
                        }
                    }
                    break;
                }
                // reverse the list before returning it
                ret.Reverse();
            }
            return ret;
        }

        public float GetCost(int origin, int destination)
        {
            return _links[(origin, destination)].GeneralCost;
        }

        public float GetTime(int origin, int destination)
        {
            return _links[(origin, destination)].TravelTime;
        }
    }
}
