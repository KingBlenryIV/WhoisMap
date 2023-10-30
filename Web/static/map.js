function getRandomColor() {
    var letters = '0123456789ABCDEF';
    var color = '#';
    for (var i = 0; i < 6; i++) {
        color += letters[Math.floor(Math.random() * 16)];
    }
    return color;
}

function loadPage() {
    map.touchZoom.enable();
    map.doubleClickZoom.enable();
    map.scrollWheelZoom.enable();
    map.boxZoom.enable();
    map.keyboard.enable();
    $(".leaflet-control-zoom").css("visibility", "visible");
    $(".leaflet-control-layers").css("visibility", "visible");
    $(".loader").css("visibility", "hidden");
}

function createLayers(data) {
    data.forEach(net => {
        var color = getRandomColor()
        var latlong = net["coords"].split(",")
        var tooltipText = "Org: " + net["org"] + "<br>" + "Start: " + net["start"] + "<br>" + "End: " + net["end"] + "<br>" + "Size: /" + net["cidr"]
        if (net["v"] == 4) {
            cidr = parseFloat(net["cidr"])
            radius = 10.0/(0.002*(cidr-3.0)**2.0)
            var circle = L.circle([latlong[0],latlong[1]], {radius: radius, color: color, fill: false, weight: 5})
            circle.bindTooltip(tooltipText).openTooltip();
            ipv4LayerGroup.addLayer(circle)
        }
        if (net["v"] == 6) {
            cidr = parseFloat(net["cidr"])
            radius = 10.0/(0.003*(cidr-10.0)**1.6)
            var circle = L.circle([latlong[0],latlong[1]], {radius: radius, color: color, fill: false, weight: 5})
            circle.bindTooltip(tooltipText).openTooltip();
            ipv6LayerGroup.addLayer(circle)
        }
    });
    map.addLayer(ipv4LayerGroup)
    map.addLayer(ipv6LayerGroup)
    loadPage();
}

// Initialize
var map = L.map('map').setView([39.26, -95.94], 5);
const tiles = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    noWrap: true,
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);

var ipv4LayerGroup = L.markerClusterGroup({disableClusteringAtZoom: 13, animate: false});
var ipv6LayerGroup = L.markerClusterGroup({disableClusteringAtZoom: 13, animate: false});

var overlays = {
    "IPv4": ipv4LayerGroup,
    "IPv6": ipv6LayerGroup
}

var layerControl = L.control.layers(null, overlays).addTo(map);

map.touchZoom.disable();
map.doubleClickZoom.disable();
map.scrollWheelZoom.disable();
map.boxZoom.disable();
map.keyboard.disable();
$(".leaflet-control-zoom").css("visibility", "hidden");
$(".leaflet-control-layers").css("visibility", "hidden");


function checkIfDataExists(db, key, name, callback) {
    const transaction = db.transaction([name], 'readonly');
    const objectStore = transaction.objectStore(name);
  
    const request = objectStore.get(key);
  
    request.onsuccess = (event) => {
        const result = event.target.result;
        callback(result);
    };
  
    request.onerror = () => {
        console.error('Error checking if data exists in IndexedDB');
        callback(null);
    };
  }
  
function fetchDataAndStore() {
    const dbName = 'map_json';
    const dbVersion = 1;

    const request = indexedDB.open(dbName, dbVersion);

    request.onsuccess = (event) => {
        const db = event.target.result;

        checkIfDataExists(db, "map", "map", (result) => {
            if (result) {
                console.log('Map is already downloaded');
                createLayers(result.data)
            } else {
                fetch('http://'+location.host+'/static/map.json')
                .then((response) => response.json())
                .then((data) => {
                    const transaction = db.transaction(["map"], 'readwrite');
                    const objectStore = transaction.objectStore("map");

                    const jsonToStore = {
                        key: "map",
                        data: data,
                    };

                    const addObjectStoreRequest = objectStore.put(jsonToStore);

                    addObjectStoreRequest.onsuccess = () => {
                        console.log('Map JSON data stored in IndexedDB');
                        createLayers(data)
                    };

                    addObjectStoreRequest.onerror = () => {
                        console.error('Error storing map JSON data in IndexedDB');
                    };
                })
                .catch((error) => {
                    console.error('Error fetching JSON data', error);
                });
            }
        });
    };

    request.onerror = (event) => {
        console.error('Error opening database', event.target.error);
    };

    request.onupgradeneeded = (event) => {
        const db = event.target.result;

        if (!db.objectStoreNames.contains("map")) {
            db.createObjectStore("map", { keyPath: 'key' });
        }
    };
}

fetchDataAndStore();