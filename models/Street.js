const mongoose = require('mongoose');

const streetSchema = new mongoose.Schema({
  name: {
    type: String,
    required: [true, 'Street name is required'],
    trim: true,
    unique: true
  },
  createdAt: {
    type: Date,
    default: Date.now
  }
});

module.exports = mongoose.model('Street', streetSchema);
