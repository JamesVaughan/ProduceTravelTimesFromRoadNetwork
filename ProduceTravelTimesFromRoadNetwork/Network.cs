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
        internal readonly float Distance;
        public float GeneralCost;
        public float TravelTime;

        public Link(int i, int j, float distance)
        {
            I = i;
            J = j;
            Distance = distance;
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

    struct TransitSegment
    {
        public char mode;
        public string line;
        public int node;
    }

    sealed class Network
    {
        private readonly Dictionary<int, Node> _nodes = new Dictionary<int, Node>();
        private readonly Dictionary<int, List<Node>> _centroidsInZone = new Dictionary<int, List<Node>>();
        private readonly Dictionary<(int, int), Link> _links = new Dictionary<(int, int), Link>();
        private readonly HashSet<(int i, int j, int k)> _turnRestrictions = new HashSet<(int i, int j, int k)>();
        /// <summary>
        /// (int, float) => destination node number, travel time in minutes
        /// </summary>
        private readonly Dictionary<string, List<(int, float)>> _transitPaths = new Dictionary<string, List<(int, float)>>();
        private readonly Dictionary<(int, int), List<TransitSegment>> _pathsThroughTransit = new Dictionary<(int, int), List<TransitSegment>>();

        private Network()
        {

        }

        private void Add(Node node)
        {
            node.LinksTo = node.LinksTo ?? new List<int>();
            _nodes[node.NodeNumber] = node;
            if(node.Centroid)
            {
                if(!_centroidsInZone.TryGetValue(node.ZoneNumber, out var centoidList))
                {
                    centoidList = new List<Node>();
                    _centroidsInZone[node.ZoneNumber] = centoidList;
                }
                centoidList.Add(node);
            }
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

        private void Add(string lineName, List<(int, float)> transitPath)
        {
            _transitPaths.Add(lineName, transitPath);
        }

        private void Add(int origin, int destination, List<TransitSegment> path)
        {
            _pathsThroughTransit.Add((origin, destination), path);
        }

        public static Network LoadNetwork(string nwpPath, string shapeFile, string zoneFieldName, string transitPaths)
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
            return LoadNWP(nwpPath, LoadZones(shapeFile, zoneFieldName), transitPaths);
        }

        private static Network LoadNWP(string NWPLocation, List<(int zoneNumber, IGeometry geometry)> zones, string transitODPaths)
        {
            if (!File.Exists(NWPLocation))
            {
                return null;
            }
            Network network = new Network();
            using (ZipArchive archive = new ZipArchive(File.OpenRead(NWPLocation), ZipArchiveMode.Read, false))
            {
                ZipArchiveEntry nodes = null, links = null, exAtt = null, turns = null, transitSegments = null, transit = null;
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
                    else if (entry.Name.Equals("turns.231", StringComparison.InvariantCultureIgnoreCase))
                    {
                        turns = entry;
                    }
                    else if (entry.Name.Equals("transit.221", StringComparison.InvariantCultureIgnoreCase))
                    {
                        transitSegments = entry;
                    }
                    else if (entry.Name.Equals("segment_results.csv", StringComparison.InvariantCultureIgnoreCase))
                    {
                        transit = entry;
                    }
                }
                Parallel.Invoke(
                    () =>
                    {
                        LoadNodes(nodes, network, zones);
                        LoadLinks(links, exAtt, network);
                        LoadTurns(turns, network);
                        if (transit != null)
                        {
                            LoadTransitSegments(transitSegments, network);
                            LoadTransitResults(transit, network);
                        }
                    },
                    () => LoadPaths(transitODPaths, network));
                return network;
            }
        }

        internal bool PickCentroidInZone(ref int zone, Random r)
        {
            if(!_centroidsInZone.TryGetValue(zone, out var centroids))
            {
                return false;
            }
            zone = centroids[r.Next(centroids.Count)].NodeNumber;
            return true;
        }

        internal float GetDistance(int origin, int destination)
        {
            if (origin <= -1 || origin == destination)
            {
                return 0f;
            }
            // See if the two nodes are connected
            if (_links.TryGetValue((origin, destination), out var link))
            {
                return link.Distance;
            }
            // if the links are not connected, use a straight line distance between the two points.
            return ComputeDistance(origin, destination);
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
                        var o = int.Parse(parts[0]);
                        var d = int.Parse(parts[1]);
                        network.Add(new Link(o, d, ComputetDistance(network, o, d))
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

        private static float ComputetDistance(Network network, int originNode, int destinationNode)
        {
            var o = network._nodes[originNode];
            var d = network._nodes[destinationNode];
            var deltaX = (o.X - d.X);
            var deltaY = (o.Y - d.Y);
            return (float)Math.Sqrt(deltaX * deltaX + deltaY * deltaY);
        }

        internal float ComputeDistance(int origin, int destination) => Network.ComputetDistance(this, origin, destination);

        private static void LoadTurns(ZipArchiveEntry turnsArchive, Network network)
        {
            using (var reader = new StreamReader(turnsArchive.Open()))
            {
                string line = null;
                const string nodesMarker = "t turns";
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

        private static void LoadTransitResults(ZipArchiveEntry transitArchive, Network network)
        {
            using (var reader = new StreamReader(transitArchive.Open()))
            {
                // burn header
                reader.ReadLine();
                string line;
                var seperators = new char[] { ',', '\t' };
                var currentPath = new List<(int, float)>();
                while ((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(seperators);
                    if (parts.Length >= 7)
                    {
                        var i = int.Parse(parts[1]);
                        var j = int.Parse(parts[2]);
                        var loop = int.Parse(parts[3]);
                        var travelTime = float.Parse(parts[5]);
                        var segments = network._transitPaths[parts[0]];
                        int currentLoop = 0;
                        var prev = segments[0].Item1;
                        for (int seg = 1; seg < segments.Count; seg++)
                        {
                            if(segments[seg].Item1 == j && prev == i)
                            {
                                if(++currentLoop == loop)
                                {
                                    // update the segment with a travel time
                                    segments[seg] = (segments[seg].Item1, travelTime);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }


        private static void LoadTransitSegments(ZipArchiveEntry transitArchive, Network network)
        {
            using (var reader = new StreamReader(transitArchive.Open()))
            {
                string line = null;
                const string nodesMarker = "t lines";
                while ((line = reader.ReadLine()) != null && line != nodesMarker) ;
                if (line != nodesMarker)
                {
                    return;
                }
                // burn the header
                reader.ReadLine();
                var seperators = new char[] { ' ', '\t' };
                string currentLine = null;
                var currentPath = new List<(int, float)>();
                void Store()
                {
                    if (currentLine != null)
                    {
                        network.Add(currentLine, currentPath);
                        currentLine = null;
                        // this needs to be a new object
                        currentPath = new List<(int, float)>();
                    }
                }
                while ((line = reader.ReadLine()) != null && line[0] != 't')
                {
                    // ignore blank lines
                    if (line.Length > 2)
                    {
                        if (line[0] == 'c')
                        {
                            continue;
                        }
                        else if (line[0] == 'a')
                        {
                            Store();
                            // a'B00001' b   1  36.00  18.60 '402B                '      0      0      0
                            currentLine = line.Substring(2, line.IndexOf('\'', 2) - 2);
                        }
                        else
                        {
                            var split = line.Split(seperators, StringSplitOptions.RemoveEmptyEntries);
                            if (split.Length > 0)
                            {
                                // the path line, just ignore
                                if (split[0].Length > 0 && split[0][0] == 'p')
                                {
                                    continue;
                                }
                                else
                                {
                                    if (int.TryParse(split[0], out var node))
                                    {
                                        // we will get the transit time from the results
                                        currentPath.Add((node, 0f));
                                    }
                                }
                            }
                        }
                    }
                }
                Store();
            }
        }


        private static void LoadPaths(string transitODPaths, Network network)
        {
            /*
                c all paths
                c
                c orig dest pathnum prop imped twaitime tinvtime tauxtime dist orig <aux. transit> <transit>
                c      <aux. transit>  mode -    node
                c      <transit>       mode line node
             */
            using (var reader = new StreamReader(transitODPaths))
            {
                string line = null;
                var seperators = new char[] { ' ', '\t' };
                while ((line = reader.ReadLine()) != null)
                {
                    var length = line.Length;
                    // skip lines that are comments
                    if (length > 0 && line[0] == 'c')
                    {
                        continue;
                    }
                    var parts = line.Split(seperators);
                    // AuxTransit: mode - node
                    // Transit   : mode line node
                    const int startOfPaths = 10;
                    const int partsPerSegment = 3;
                    if (parts.Length > startOfPaths)
                    {
                        // ignore paths that are not the first path
                        if (int.TryParse(parts[0], out var origin)
                            && int.TryParse(parts[1], out var destination)
                            && int.TryParse(parts[2], out var pathNumber) && pathNumber == 1)
                        {
                            // prop[3], imped[4], twaittime[5], tinvtime[6], tauxtime[7], dist[8]
                            // startNode[9] (same as origin)
                            List<TransitSegment> path = new List<TransitSegment>(Math.Max(0, (parts.Length - startOfPaths) / partsPerSegment))
                            {
                                new TransitSegment() { node = origin }
                            };
                            for (int i = startOfPaths; i < parts.Length - partsPerSegment; i += partsPerSegment)
                            {
                                path.Add(new TransitSegment()
                                {
                                    mode = parts[i][0],
                                    // make sure to remove the ''s around the line name if it is a line
                                    line = parts[i + 1].Length < 3 ? parts[i + 1] : parts[i + 1].Substring(1, parts[i + 1].Length - 2),
                                    node = int.Parse(parts[i + 2])
                                });
                            }
                            network.Add(origin, destination, path);
                        }
                    }
                }
            }
        }

        internal bool HasNode(int node)
        {
            return _nodes.ContainsKey(node);
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
            private HashSet<(int origin, int destination)> _contained = new HashSet<(int origin, int destination)>();

            public int Count => _data.Count;

            public ((int origin, int destination) link, (int parentOrigin, int parentDestination) parentLink, float cost) PopMin()
            {
                var tailIndex = _data.Count - 1;
                if (tailIndex < 0)
                {
                    return ((-1, -1), (-1, -1), -1);
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
                _contained.Remove(top.link);
                _data.RemoveAt(_data.Count - 1);
                return (top.link, top.parentLink, top.cost);
            }

            public void Push((int origin, int destination) link, (int parentOrigin, int parentDestination) parentLink, float cost)
            {
                int current = _data.Count;
                if (_contained.Contains(link))
                {
                    for (current = 0; current < _data.Count; current++)
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
                }
                if (current == _data.Count)
                {
                    // if it is not already contained
                    _data.Add((link, parentLink, cost));
                    _contained.Add(link);
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
                if (!fastestParent.TryAdd(current.link, current.parentLink))
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
                var node = _nodes[currentDestination];
                var links = node.LinksTo;
                foreach (var childDestination in links)
                {
                    // explore everything that hasn't been solved, the min heap will update if it is a faster path to the child node
                    (int currentDestination, int childDestination) nextStep = (currentDestination, childDestination);
                    if (!fastestParent.ContainsKey(nextStep))
                    {
                        // don't explore centroids that are not our destination
                        if (!_nodes[childDestination].Centroid || childDestination == destinationZoneNumber)
                        {
                            // ensure there is not a turn restriction
                            if (!_turnRestrictions.Contains((current.link.origin, currentDestination, childDestination)))
                            {
                                // make sure cars are allowed on the link
                                var linkCost = _links[nextStep].GeneralCost;
                                if (linkCost >= 0)
                                {
                                    toExplore.Push(nextStep, current.link, current.cost + linkCost);
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
            if (origin <= -1)
            {
                return 0f;
            }
            return _links[(origin, destination)].GeneralCost;
        }

        public float GetTime(int origin, int destination)
        {
            if (origin <= -1)
            {
                return 0f;
            }
            return _links[(origin, destination)].TravelTime;
        }

        public int GetZone(int nodeNumber)
        {
            return _nodes[nodeNumber].ZoneNumber;
        }

        public List<(int, float)> GetTransitTravelOnRouteSegments(string routeName, int originOnRoute, int destinationOnRoute)
        {           
            if (!_transitPaths.TryGetValue(routeName, out var path))
            {
                return null;
            }
            var ret = new List<(int, float)>();
            // find the origin point
            int i = 0;
            for (; i < path.Count; i++)
            {
                if (path[i].Item1 == originOnRoute)
                {
                    ret.Add(path[i]);
                    break;
                }
            }
            // store until you find the destination
            for (i = i + 1; i < path.Count; i++)
            {
                if (path[i].Item1 == originOnRoute)
                {
                    ret.Clear();
                }
                ret.Add(path[i]);
                if (path[i].Item1 == destinationOnRoute)
                {
                    break;
                }
            }
            return ret;
        }

        public IReadOnlyList<TransitSegment> GetPathThroughTransit(int origin, int destination)
        {
            _pathsThroughTransit.TryGetValue((origin, destination), out var ret);
            return ret;
        }
    }
}
