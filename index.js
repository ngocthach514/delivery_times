const axios = require('axios');
const mysql = require('mysql2/promise');
const express = require('express');
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
  return address.trim().toLowerCase().replace(/\s+/g, ' ');
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

// Hàm kiểm tra tọa độ trong cache (MySQL)
async function getCachedCoordinates(address) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    const [rows] = await connection.execute(
      'SELECT coordinates FROM delivery_times WHERE address = ? AND coordinates IS NOT NULL AND is_cached = TRUE LIMIT 1',
      [address]
    );
    return rows.length > 0 ? rows[0].coordinates.split(',').map(Number) : null;
  } catch (error) {
    console.error(`Lỗi kiểm tra cache (${address}):`, error.message);
    return null;
  } finally {
    await connection.end();
  }
}

// Hàm lưu tọa độ vào cache
async function saveToCache(address, coords) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    const [existing] = await connection.execute(
      'SELECT id FROM delivery_times WHERE address = ? AND coordinates = ?',
      [address, coords.join(',')]
    );
    if (existing.length === 0) {
      await connection.execute(
        'INSERT INTO delivery_times (address_index, address, coordinates, is_cached) VALUES (?, ?, ?, ?)',
        [0, address, coords.join(','), true]
      );
      console.log(`Đã lưu tọa độ vào cache: ${address} -> ${coords}`);
    }
  } catch (error) {
    console.error(`Lỗi lưu cache (${address}):`, error.message);
  } finally {
    await connection.end();
  }
}

// Hàm chuyển địa chỉ thành tọa độ bằng TomTom Search API
async function geocodeAddress(address, retries = 2) {
  const cachedCoords = await getCachedCoordinates(address);
  if (cachedCoords && isValidCoordinate(cachedCoords)) {
    console.log(`Dùng tọa độ cache cho: ${address}`);
    return cachedCoords;
  }

  const fullAddress = `${address}, Việt Nam`;
  const encodedAddress = encodeURIComponent(fullAddress);
  const url = `https://api.tomtom.com/search/2/geocode/${encodedAddress}.json?key=${apiKey}&limit=1`;

  for (let i = 0; i <= retries; i++) {
    try {
      const response = await axios.get(url);
      geocodingCount++;
      if (response.data.results && response.data.results.length > 0) {
        const coords = [response.data.results[0].position.lon, response.data.results[0].position.lat];
        if (!isValidCoordinate(coords)) {
          throw new Error(`Tọa độ không hợp lệ cho TP.HCM: ${coords}`);
        }
        await saveToCache(address, coords);
        console.log(`Geocoding: ${address} -> ${coords}`);
        return coords;
      }
      throw new Error(`Không tìm thấy tọa độ cho địa chỉ: ${address}`);
    } catch (error) {
      const errorDetails = error.response ? JSON.stringify(error.response.data, null, 2) : error.message;
      console.error(`Lỗi Geocoding (${address}, lần ${i + 1}): ${errorDetails}`);
      if (i < retries) {
        await new Promise((resolve) => setTimeout(resolve, 3000 * (i + 1)));
      } else {
        await saveFailedGeocoding(address, 'Unknown', errorDetails);
        return null;
      }
    }
  }
}

// Hàm lưu địa chỉ geocoding thất bại
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
  const coords = await geocodeAddress(normalizeAddress(warehouseAddress));
  if (!coords) {
    throw new Error('Không thể lấy tọa độ kho');
  }
  return coords;
}

// Hàm chia mảng thành các chunk
const chunkArray = (array, size) => {
  const result = [];
  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }
  return result;
};

// Hàm tính thời gian và khoảng cách bằng TomTom Calculate Route API cho xe máy
async function getTravelTimes(locations, originalItems) {
  const results = [];
  
  for (let i = 0; i < originalItems.length; i++) {
    const item = originalItems[i];
    const destination = locations[i + 1]; // Địa chỉ giao hàng
    const origin = locations[0]; // Tọa độ kho
    
    // Thêm thời gian chờ giữa các yêu cầu để tránh lỗi 429
    await new Promise(resolve => setTimeout(resolve, i * 1000));
    
    const url = `https://api.tomtom.com/routing/1/calculateRoute/${origin[1]},${origin[0]}:${destination[1]},${destination[0]}/json?key=${apiKey}&travelMode=motorcycle`;
    
    try {
      const response = await axios.get(url);
      routingCount++;

      if (response.data.routes && response.data.routes.length > 0) {
        const route = response.data.routes[0];
        results.push({
          order_id: item.order_id,
          address: item.address,
          coordinates: destination,
          duration: route.summary.travelTimeInSeconds / 60, // phút
          distance: route.summary.lengthInMeters / 1000 // km
        });
      } else {
        throw new Error('Không tìm thấy tuyến đường');
      }
    } catch (error) {
      console.error(`Lỗi Calculate Route (${item.address}):`, error.response ? JSON.stringify(error.response.data, null, 2) : error.message);
      results.push({
        order_id: item.order_id,
        address: item.address,
        coordinates: null,
        duration: null,
        distance: null
      });
    }
  }
  
  return results;
}

// Hàm trích xuất phường/xã và quận/huyện
function extractWardDistrict(address) {
  const wardMatch = address.match(/(Phường\s+[^,]+)/i) || address.match(/P\.\s*[^,]+/i);
  const districtMatch = address.match(/(Quận\s+[^,]+)/i) || address.match(/Q\.\s*[^,]+/i);
  return {
    ward: wardMatch ? wardMatch[0].trim() : 'Unknown',
    district: districtMatch ? districtMatch[0].trim() : 'Unknown',
  };
}

// Hàm lưu vào MySQL (delivery_times) với kiểm tra trùng coordinates
async function saveToMySQL(results) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    const values = [];
    for (let i = 0; i < results.length; i++) {
      const result = results[i];
      if (result.coordinates) {
        const [existing] = await connection.execute(
          'SELECT id FROM delivery_times WHERE coordinates = ?',
          [result.coordinates.join(',')]
        );
        if (existing.length === 0) {
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
    }
    if (values.length > 0) {
      await connection.query(
        'INSERT INTO delivery_times (address_index, order_id, address, coordinates, duration, distance, is_cached) VALUES ?',
        [values]
      );
    }
    console.log('Đã lưu', values.length, 'bản ghi vào delivery_times');
  } catch (error) {
    console.error('Lỗi MySQL (delivery_times):', error.message);
  } finally {
    await connection.end();
  }
}

// Hàm lưu vào sorted_orders với cập nhật ghi đè
async function saveToSortedOrders(results) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    const sortedResults = results
      .filter((r) => r.duration && r.distance)
      .map((r) => ({
        ...r,
        ...extractWardDistrict(r.address),
      }))
      .sort((a, b) => {
        if (a.ward === b.ward) {
          if (a.district === b.district) {
            return a.duration - b.duration;
          }
          return a.district.localeCompare(b.district);
        }
        return a.ward.localeCompare(b.ward);
      });

    for (let i = 0; i < sortedResults.length; i++) {
      const result = sortedResults[i];
      const [existing] = await connection.execute(
        'SELECT id FROM sorted_orders WHERE order_id = ? AND DATE(created_at) = CURDATE()',
        [result.order_id]
      );
      if (existing.length > 0) {
        await connection.execute(
          'UPDATE sorted_orders SET address = ?, ward = ?, district = ?, delivery_order = ?, duration = ?, distance = ? WHERE order_id = ? AND DATE(created_at) = CURDATE()',
          [
            result.address,
            result.ward,
            result.district,
            i + 1,
            result.duration,
            result.distance,
            result.order_id,
          ]
        );
      } else {
        await connection.execute(
          'INSERT INTO sorted_orders (order_id, address, ward, district, delivery_order, duration, distance) VALUES (?, ?, ?, ?, ?, ?, ?)',
          [
            result.order_id,
            result.address,
            result.ward,
            result.district,
            i + 1,
            result.duration,
            result.distance,
          ]
        );
      }
    }
    console.log('Đã lưu/cập nhật', sortedResults.length, 'bản ghi vào sorted_orders');
  } catch (error) {
    console.error('Lỗi MySQL (sorted_orders):', error.message);
  } finally {
    await connection.end();
  }
}

// Hàm chính
async function main() {
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

  const addressCoords = [];
  const validItems = [];
  const coordPromises = items.map(async (item, index) => {
    await new Promise(resolve => setTimeout(resolve, index * 1000));
    const coords = await geocodeAddress(item.address);
    return { item, coords };
  });
  const coordResults = await Promise.all(coordPromises);

  for (const { item, coords } of coordResults) {
    if (coords && isValidCoordinate(coords)) {
      addressCoords.push(coords);
      validItems.push(item);
    }
  }

  const chunks = chunkArray(addressCoords, 25);
  const chunkItems = chunkArray(validItems, 25);
  let results = [];

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    const chunkItemsBatch = chunkItems[i];
    const chunkResults = await getTravelTimes([warehouseCoords, ...chunk], chunkItemsBatch);
    results = results.concat(chunkResults);
  }

  console.log('Kết quả:');
  results.forEach((result, i) => {
    console.log(
      `Địa chỉ ${i + 1}: ${result.address}, Order: ${result.order_id}, ` +
      `${result.duration ? result.duration.toFixed(2) + ' phút' : 'N/A'}, ` +
      `${result.distance ? result.distance.toFixed(2) + ' km' : 'N/A'}, ` +
      `Coords: ${result.coordinates || 'N/A'}`
    );
  });

  await saveToMySQL(results);
  await saveToSortedOrders(results);

  console.log(`Tổng yêu cầu: Geocoding=${geocodingCount}, Matrix Routing=${routingCount}`);
}

// Tạo API nội bộ bằng Express
const app = express();

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

// Khởi động server và chạy main, sau đó dừng chương trình
const server = app.listen(apiPort, async () => {
  console.log(`API nội bộ chạy tại http://localhost:${apiPort}`);
  try {
    await main();
    console.log('Chương trình hoàn tất, đang dừng server...');
    server.close(() => {
      console.log('Server đã dừng. Thoát chương trình.');
      process.exit(0);
    });
  } catch (error) {
    console.error('Lỗi trong main:', error);
    server.close(() => {
      console.log('Server đã dừng do lỗi. Thoát chương trình.');
      process.exit(1);
    });
  }
});