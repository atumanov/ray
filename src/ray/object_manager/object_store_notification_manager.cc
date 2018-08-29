#include <future>
#include <iostream>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "common/common.h"
#include "common/common_protocol.h"

#include "ray/object_manager/object_store_notification_manager.h"
#include "ray/util/util.h"

namespace ray {

ObjectStoreNotificationManager::ObjectStoreNotificationManager(
    boost::asio::io_service &io_service, const std::string &store_socket_name)
    : store_client_(), socket_(io_service) {
  ARROW_CHECK_OK(store_client_.Connect(store_socket_name.c_str(), "",
                                       plasma::kPlasmaDefaultReleaseDelay));

  ARROW_CHECK_OK(store_client_.Subscribe(&c_socket_));
  boost::system::error_code ec;
  socket_.assign(boost::asio::local::stream_protocol(), c_socket_, ec);
  assert(!ec.value());
  NotificationWait();
}

ObjectStoreNotificationManager::~ObjectStoreNotificationManager() {
  ARROW_CHECK_OK(store_client_.Disconnect());
}

void ObjectStoreNotificationManager::NotificationWait() {
  boost::asio::async_read(socket_, boost::asio::buffer(&length_, sizeof(length_)),
                          boost::bind(&ObjectStoreNotificationManager::ProcessStoreLength,
                                      this, boost::asio::placeholders::error));
}


void ObjectStoreNotificationManager::ProcessStoreLength(
    const boost::system::error_code &error) {
  RAY_CHECK(!error);
  int num_notifications = 0;
  auto start = current_time_ms();
  do {
    notification_.resize(length_);
    boost::asio::read(
        socket_, boost::asio::buffer(notification_));

    const auto &object_info = flatbuffers::GetRoot<ObjectInfo>(notification_.data());
    const auto &object_id = from_flatbuf(*object_info->object_id());
    //RAY_LOG(INFO) << object_id << " available at " << current_time_ms();
    if (object_info->is_deletion()) {
      ProcessStoreRemove(object_id);
    } else {
      ObjectInfoT result;
      object_info->UnPackTo(&result);
      ProcessStoreAdd(result);
    }
    num_notifications++;

	if (current_time_ms() - start > 10) {
      length_ = -1;
    } else if (socket_.available() > 0) {
      RAY_CHECK(boost::asio::read(socket_, boost::asio::buffer(&length_, sizeof(length_))) > 0);
    } else {
      length_ = -1;
    }
  } while (length_ > 0);

  NotificationWait();
}

void ObjectStoreNotificationManager::ProcessStoreAdd(const ObjectInfoT &object_info) {
  for (auto &handler : add_handlers_) {
    handler(object_info);
  }
}

void ObjectStoreNotificationManager::ProcessStoreRemove(const ObjectID &object_id) {
  for (auto &handler : rem_handlers_) {
    handler(object_id);
  }
}

void ObjectStoreNotificationManager::SubscribeObjAdded(
    std::function<void(const ObjectInfoT &)> callback) {
  add_handlers_.push_back(std::move(callback));
}

void ObjectStoreNotificationManager::SubscribeObjDeleted(
    std::function<void(const ObjectID &)> callback) {
  rem_handlers_.push_back(std::move(callback));
}

}  // namespace ray
