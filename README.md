# Street Management API

Node.js application for managing street names with MongoDB database.

## Features
- Add new street names to database
- Retrieve all streets
- Get individual street by ID
- Delete streets
- Duplicate prevention

## Setup

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Configure environment:**
   - Update `.env` file with your MongoDB connection string
   - Default: `mongodb://localhost:27017/street_management`

3. **Start MongoDB:**
   Make sure MongoDB is running on your system

4. **Run the server:**
   ```bash
   npm start
   ```
   
   Or for development with auto-reload:
   ```bash
   npm run dev
   ```

## API Endpoints

### Add a Street
**POST** `/api/streets/add`

Request body:
```json
{
  "name": "Main Street"
}
```

Response:
```json
{
  "success": true,
  "message": "Street saved successfully",
  "data": {
    "_id": "...",
    "name": "Main Street",
    "createdAt": "2024-01-01T00:00:00.000Z"
  }
}
```

### Get All Streets
**GET** `/api/streets`

Response:
```json
{
  "success": true,
  "count": 2,
  "data": [...]
}
```

### Get Street by ID
**GET** `/api/streets/:id`

### Delete Street
**DELETE** `/api/streets/:id`

## Testing with cURL

```bash
# Add a street
curl -X POST http://localhost:5000/api/streets/add -H "Content-Type: application/json" -d "{\"name\":\"Main Street\"}"

# Get all streets
curl http://localhost:5000/api/streets
```

## Project Structure
```
server/
├── models/
│   └── Street.js       # Street schema
├── routes/
│   └── streets.js      # Street routes
├── server.js           # Main server file
├── package.json
├── .env
└── README.md
```
