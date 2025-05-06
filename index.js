const axios = require('axios');
const mysql = require('mysql2/promise');
const express = require('express');
let pLimit;
try {
  pLimit = require('p-limit').default || require('p-limit');
} catch (error) {
  console.error('Lỗi: Thư viện p-limit chưa được cài đặt hoặc không tương thích. Hãy chạy: npm install p-limit');
  process.exit(1);
}
require('dotenv').config();

// Cấu hình từ .env
const apiKey = process.env.TOMTOM_API_KEY;
const warehouseAddress = process.env.WAREHOUSE_ADDRESS;
const warehouseCoordsStr = process.env.WAREHOUSE_COORDINATES;
const inputApiUrl = process.env.INPUT_API_URL;
const inputApiToken = process.env.INPUT_API_TOKEN;
const apiPort = process.env.API_PORT || 3000;

// Cấu hình MySQL từ .env
const dbConfig = {
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
};

// Giới hạn số lượng yêu cầu đồng thời và retry
const concurrencyLimit = 2;
const limit = pLimit(concurrencyLimit);
const maxRetries = 3;
const baseRetryDelay = 1000;

// Kiểm tra biến môi trường
if (!apiKey || !inputApiUrl || !dbConfig.host || !dbConfig.database || !warehouseAddress) {
  console.error('Thiếu biến môi trường cần thiết. Kiểm tra file .env');
  process.exit(1);
}

// Biến đếm yêu cầu API
let geocodingCount = 0;
let routingCount = 0;

// Hàm chuẩn hóa địa chỉ
function normalizeAddress(address) {
  let normalized = address.trim().toLowerCase().replace(/\s+/g, ' ');
  normalized = normalized.replace(/\bq(\d+)\b/g, 'quận $1');
  if (!normalized.includes('tp.hcm') && !normalized.includes('hồ chí minh')) {
    normalized += ', tp.hcm';
  }
  return normalized;
}

// Hàm lấy danh sách đơn hàng từ API đầu vào
async function fetchAddresses() {
  try {
    const config = {
      headers: {},
    };
    if (inputApiToken) {
      config.headers.Authorization = `Bearer ${inputApiToken}`;
    }
    const response = await axios.get(inputApiUrl, config);
    return response.data.map((item) => ({
      order_id: item.order_id,
      address: normalizeAddress(item.address),
    }));
  } catch (error) {
    console.error('Lỗi khi lấy đơn hàng từ API:', error.response ? error.response.data : error.message);
    return [];
  }
}

// Hàm kiểm tra tọa độ hợp lệ cho TP.HCM
function isValidCoordinate(coords) {
  if (!coords || coords.length !== 2) return false;
  const [lng, lat] = coords;
  return lng >= 106.5 && lng <= 106.8 && lat >= 10.6 && lat <= 10.9;
}

// Hàm kiểm tra tọa độ và thông tin tuyến đường trong cache
async function getCachedData(addresses) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    const placeholders = addresses.map(() => '?').join(',');
    const [rows] = await connection.execute(
      `SELECT address, order_id, coordinates, duration, distance 
       FROM delivery_times 
       WHERE address IN (${placeholders}) AND coordinates IS NOT NULL AND is_cached = TRUE`,
      addresses
    );
    const cacheMap = new Map();
    rows.forEach(row => {
      cacheMap.set(row.address, {
        order_id: row.order_id,
        coordinates: row.coordinates.split(',').map(Number),
        duration: row.duration,
        distance: row.distance
      });
    });
    return cacheMap;
  } catch (error) {
    console.error('Lỗi kiểm tra cache:', error.message);
    return new Map();
  } finally {
    await connection.end();
  }
}

// Hàm lưu tọa độ và thông tin tuyến đường vào cache
async function saveToCache(data) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    const values = [];
    for (const { order_id, address, coordinates, duration, distance } of data) {
      // Kiểm tra trùng lặp dựa trên order_id và coordinates
      const [existing] = await connection.execute(
        'SELECT id FROM delivery_times WHERE order_id = ? AND coordinates = ?',
        [order_id, coordinates.join(',')]
      );
      if (existing.length === 0) {
        values.push([0, order_id, address, coordinates.join(','), duration, distance, true]);
      } else {
        console.log(`Bỏ qua chèn bản ghi vì trùng order_id ${order_id} và tọa độ ${coordinates.join(',')}`);
      }
    }
    if (values.length > 0) {
      await connection.query(
        'INSERT INTO delivery_times (address_index, order_id, address, coordinates, duration, distance, is_cached) VALUES ?',
        [values]
      );
      console.log(`Đã lưu ${values.length} bản ghi vào cache`);
    }
  } catch (error) {
    console.error('Lỗi lưu cache:', error.message);
  } finally {
    await connection.end();
  }
}

// Hàm chuyển địa chỉ thành tọa độ bằng TomTom Search API
async function geocodeAddress(address, retries = maxRetries) {
  const fullAddress = `${address}, Việt Nam`;
  const encodedAddress = encodeURIComponent(fullAddress);
  const url = `https://api.tomtom.com/search/2/geocode/${encodedAddress}.json?key=${apiKey}&limit=1`;

  for (let i = 0; i <= retries; i++) {
    try {
      const response = await axios.get(url);
      geocodingCount++;
      if (response.data.results && response.data.results.length > 0) {
        const result = response.data.results[0];
        const coords = [result.position.lon, result.position.lat];
        if (!isValidCoordinate(coords)) {
          throw new Error(`Tọa độ không hợp lệ cho TP.HCM: ${coords}`);
        }
        const ward = result.address.municipalitySubdivision || 'Unknown';
        console.log(`Geocoding: ${address} -> ${coords}, Phường: ${ward}`);
        return { coords, ward };
      }
      throw new Error(`Không tìm thấy tọa độ cho địa chỉ: ${address}`);
    } catch (error) {
      const errorDetails = error.response ? JSON.stringify(error.response.data, null, 2) : error.message;
      if (error.response && error.response.status === 429) {
        if (i < retries) {
          const delay = baseRetryDelay * Math.pow(2, i);
          console.warn(`Rate limit đạt được cho ${address}. Thử lại sau ${delay}ms...`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
      }
      console.error(`Lỗi Geocoding (${address}, lần ${i + 1}): ${errorDetails}`);
      await saveFailedGeocoding(address, 'Unknown', errorDetails);
      return null;
    }
  }
}

// Hàm lưu địa chỉ thất bại
async function saveFailedGeocoding(address, order_id, error_message) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    const [tables] = await connection.execute("SHOW TABLES LIKE 'failed_geocoding'");
    if (tables.length === 0) {
      console.warn('Bảng failed_geocoding không tồn tại. Vui lòng tạo bảng.');
      return;
    }
    await connection.execute(
      'INSERT INTO failed_geocoding (order_id, address, error_message) VALUES (?, ?, ?)',
      [order_id, address, error_message]
    );
    console.log(`Đã lưu địa chỉ thất bại vào failed_geocoding: ${address}`);
  } catch (error) {
    console.error('Lỗi lưu failed_geocoding:', error.message);
  } finally {
    await connection.end();
  }
}

// Hàm lấy tọa độ kho
async function getWarehouseCoordinates() {
  if (warehouseCoordsStr) {
    const coords = warehouseCoordsStr.split(',').map(Number);
    if (isValidCoordinate(coords)) {
      console.log(`Dùng tọa độ kho từ .env: ${coords}`);
      return coords;
    }
    console.warn('Tọa độ kho trong .env không hợp lệ, chuyển sang Geocoding');
  }
  const result = await geocodeAddress(normalizeAddress(warehouseAddress));
  if (!result || !result.coords) {
    throw new Error('Không thể lấy tọa độ kho');
  }
  return result.coords;
}

// Hàm chia mảng thành các chunk
const chunkArray = (array, size) => {
  const result = [];
  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }
  return result;
};

// Hàm tính thời gian và khoảng cách bằng TomTom Calculate Route API
async function getTravelTimes(locations, originalItems) {
  const results = [];
  const promises = originalItems.map((item, i) =>
    limit(async () => {
      const destination = locations[i + 1];
      const origin = locations[0];
      const url = `https://api.tomtom.com/routing/1/calculateRoute/${origin[1]},${origin[0]}:${destination[1]},${destination[0]}/json?key=${apiKey}&travelMode=motorcycle`;

      for (let retry = 0; retry <= maxRetries; retry++) {
        try {
          const response = await axios.get(url);
          routingCount++;
          if (response.data.routes && response.data.routes.length > 0) {
            const route = response.data.routes[0];
            return {
              order_id: item.order_id,
              address: item.address,
              coordinates: destination,
              duration: route.summary.travelTimeInSeconds / 60,
              distance: route.summary.lengthInMeters / 1000
            };
          }
          throw new Error('Không tìm thấy tuyến đường');
        } catch (error) {
          const errorDetails = error.response ? JSON.stringify(error.response.data, null, 2) : error.message;
          if (error.response && error.response.status === 429) {
            if (retry < maxRetries) {
              const delay = baseRetryDelay * Math.pow(2, retry);
              console.warn(`Rate limit đạt được cho routing ${item.address}. Thử lại sau ${delay}ms...`);
              await new Promise(resolve => setTimeout(resolve, delay));
              continue;
            }
          }
          console.error(`Lỗi Calculate Route (${item.address}, lần ${retry + 1}): ${errorDetails}`);
          await saveFailedGeocoding(item.address, item.order_id, `Routing failed: ${errorDetails}`);
          return {
            order_id: item.order_id,
            address: item.address,
            coordinates: null,
            duration: null,
            distance: null
          };
        }
      }
    })
  );

  const chunkResults = await Promise.all(promises);
  results.push(...chunkResults);
  return results;
}

// Hàm trích xuất phường/xã và quận/huyện
function extractWardDistrict(address, geocodeWard = 'Unknown') {
  let ward = geocodeWard;
  const wardMatch = address.match(/(phường\s+[^,]+)/i) || address.match(/p\.\s*[^,]+/i);
  if (wardMatch) {
    ward = wardMatch[0].trim();
  }

  let district = 'Unknown';
  const districtMatch = address.match(/(quận\s+[^,]+)/i) || address.match(/q\.\s*[^,]+/i) || address.match(/quận\s*\d+/i);
  if (districtMatch) {
    district = districtMatch[0].trim();
  }

  return { ward, district };
}

// Hàm lưu vào MySQL (delivery_times)
async function saveToMySQL(results) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    const values = [];
    for (let i = 0; i < results.length; i++) {
      const result = results[i];
      if (result.coordinates) {
        values.push([
          i + 1,
          result.order_id,
          result.address,
          result.coordinates.join(','),
          result.duration,
          result.distance,
          true
        ]);
      }
    }
    if (values.length > 0) {
      await connection.query(
        'INSERT INTO delivery_times (address_index, order_id, address, coordinates, duration, distance, is_cached) VALUES ?',
        [values]
      );
      console.log('Đã lưu', values.length, 'bản ghi vào delivery_times');
    }
  } catch (error) {
    console.error('Lỗi MySQL (delivery_times):', error.message);
  } finally {
    await connection.end();
  }
}

// Hàm lưu vào sorted_orders với delivery_order riêng cho từng quận/phường
async function saveToSortedOrders(results) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    const groupedResults = results
      .filter((r) => r.duration && r.distance)
      .map((r) => ({
        ...r,
        ...extractWardDistrict(r.address, r.geocodeWard || 'Unknown'),
      }))
      .reduce((acc, result) => {
        const district = result.district || 'Unknown';
        const ward = result.ward || 'Unknown';
        
        if (!acc[district]) {
          acc[district] = {};
        }
        if (!acc[district][ward]) {
          acc[district][ward] = [];
        }
        acc[district][ward].push(result);
        return acc;
      }, {});

    await connection.execute('DELETE FROM sorted_orders WHERE DATE(created_at) = CURDATE()');

    const values = [];
    for (const district in groupedResults) {
      for (const ward in groupedResults[district]) {
        const sortedOrders = groupedResults[district][ward].sort((a, b) => a.distance - b.distance);
        
        for (let i = 0; i < sortedOrders.length; i++) {
          const result = sortedOrders[i];
          values.push([
            result.order_id,
            result.address,
            result.ward,
            result.district,
            i + 1,
            result.duration,
            result.distance,
          ]);
        }
      }
    }

    if (values.length > 0) {
      await connection.query(
        'INSERT INTO sorted_orders (order_id, address, ward, district, delivery_order, duration, distance) VALUES ?',
        [values]
      );
      console.log(`Đã lưu ${values.length} bản ghi vào sorted_orders với delivery_order riêng cho từng quận/phường`);
    }
  } catch (error) {
    console.error('Lỗi MySQL (sorted_orders):', error.message);
  } finally {
    await connection.end();
  }
}

// Hàm chính
async function main() {
  const startTime = Date.now();
  const items = await fetchAddresses();
  if (items.length === 0) {
    console.error('Không lấy được đơn hàng từ API. Kết thúc.');
    return;
  }
  console.log(`Đã lấy ${items.length} đơn hàng từ API`);

  let warehouseCoords;
  try {
    warehouseCoords = await getWarehouseCoordinates();
    if (!warehouseCoords) {
      throw new Error('Không thể lấy tọa độ kho');
    }
  } catch (error) {
    console.error('Lỗi lấy tọa độ kho:', error.message);
    return;
  }

  const addresses = items.map(item => item.address);
  const cachedData = await getCachedData(addresses);
  const addressCoords = [];
  const validItems = [];
  const dataToCache = [];

  const geocodePromises = items.map(item =>
    limit(async () => {
      if (cachedData.has(item.address)) {
        const cached = cachedData.get(item.address);
        console.log(`Dùng dữ liệu cache cho: ${item.address}`);
        if (cached.duration && cached.distance) {
          return {
            item,
            coords: cached.coordinates,
            geocodeWard: 'Unknown',
            duration: cached.duration,
            distance: cached.distance
          };
        }
        return { item, coords: cached.coordinates, geocodeWard: 'Unknown' };
      }
      const result = await geocodeAddress(item.address);
      if (result && result.coords && isValidCoordinate(result.coords)) {
        return { item, coords: result.coords, geocodeWard: result.ward };
      }
      return { item, coords: null, geocodeWard: 'Unknown' };
    })
  );

  const coordResults = await Promise.all(geocodePromises);

  for (const { item, coords, geocodeWard, duration, distance } of coordResults) {
    if (coords && isValidCoordinate(coords)) {
      addressCoords.push(coords);
      validItems.push({ ...item, geocodeWard, duration, distance });
      if (!cachedData.has(item.address) || (!cachedData.get(item.address).duration && !cachedData.get(item.address).distance)) {
        dataToCache.push({ order_id: item.order_id, address: item.address, coordinates: coords, duration, distance });
      }
    }
  }

  if (dataToCache.length > 0) {
    await saveToCache(dataToCache);
  }

  const chunks = chunkArray(addressCoords, 10);
  const chunkItems = chunkArray(validItems, 10);
  let results = [];

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    const chunkItemsBatch = chunkItems[i].filter(item => !item.duration || !item.distance);
    if (chunkItemsBatch.length === 0) {
      results = results.concat(chunkItems[i].map(item => ({
        order_id: item.order_id,
        address: item.address,
        coordinates: chunk[chunkItems[i].indexOf(item)],
        duration: item.duration,
        distance: item.distance,
        geocodeWard: item.geocodeWard
      })));
      continue;
    }
    const chunkCoords = [warehouseCoords, ...chunk.filter((_, idx) => chunkItemsBatch.includes(chunkItems[i][idx]))];
    const chunkResults = await getTravelTimes(chunkCoords, chunkItemsBatch);
    results = results.concat(chunkResults.map((result, idx) => ({
      ...result,
      geocodeWard: chunkItemsBatch[idx].geocodeWard
    })));
    if (i < chunks.length - 1) {
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  console.log('Kết quả:');
  results.forEach((result, i) => {
    console.log(
      `Địa chỉ ${i + 1}: ${result.address}, Order: ${result.order_id}, ` +
      `${result.duration ? result.duration.toFixed(2) + ' phút' : 'N/A'}, ` +
      `${result.distance ? result.distance.toFixed(2) + ' km' : 'N/A'}, ` +
      `Coords: ${result.coordinates || 'N/A'}, Phường (geocode): ${result.geocodeWard}`
    );
  });

  await saveToMySQL(results);
  await saveToSortedOrders(results);

  console.log(`Tổng yêu cầu: Geocoding=${geocodingCount}, Routing=${routingCount}`);
  console.log(`Thời gian xử lý: ${(Date.now() - startTime) / 1000} giây`);
}

// Tạo API nội bộ bằng Express
const app = express();

// Endpoint hiện tại để lấy danh sách đơn hàng
app.get('/orders', async (req, res) => {
  const connection = await mysql.createConnection(dbConfig);
  try {
    const [rows] = await connection.execute('SELECT order_id, address FROM orders');
    res.json(rows);
  } catch (error) {
    console.error('Lỗi khi lấy đơn hàng từ MySQL:', error.message);
    res.status(500).json({ error: 'Không thể lấy đơn hàng' });
  } finally {
    await connection.end();
  }
});

// Endpoint để lấy sorted_orders theo ngày hiện tại
app.get('/sorted-orders', async (req, res) => {
  const connection = await mysql.createConnection(dbConfig);
  try {
    const [rows] = await connection.execute(
      `SELECT order_id, address, ward, district, delivery_order, duration, distance, created_at
       FROM sorted_orders
       WHERE DATE(created_at) = CURDATE()
       ORDER BY district, ward, distance ASC`
    );

    const groupedOrders = rows.reduce((acc, order) => {
      const district = order.district || 'Unknown';
      const ward = order.ward || 'Unknown';

      if (!acc[district]) {
        acc[district] = {};
      }
      if (!acc[district][ward]) {
        acc[district][ward] = [];
      }
      acc[district][ward].push({
        order_id: order.order_id,
        address: order.address,
        delivery_order: order.delivery_order,
        duration: order.duration,
        distance: order.distance,
        created_at: order.created_at,
      });

      return acc;
    }, {});

    res.json(groupedOrders);
  } catch (error) {
    console.error('Lỗi khi lấy sorted_orders từ MySQL:', error.message);
    res.status(500).json({ error: 'Không thể lấy danh sách đơn hàng đã sắp xếp' });
  } finally {
    await connection.end();
  }
});

// Khởi động server và chạy main
app.listen(apiPort, async () => {
  console.log(`API chạy tại http://localhost:${apiPort}`);
  console.log(`Endpoint sorted_orders: http://localhost:${apiPort}/sorted-orders`);
  try {
    await main();
    console.log('Chương trình chính hoàn tất. Server tiếp tục chạy...');
  } catch (error) {
    console.error('Lỗi trong main:', error);
  }
});