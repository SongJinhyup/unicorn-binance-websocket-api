#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: unicorn_binance_websocket_api/unicorn_binance_websocket_api_restclient.py
#
# Part of ‘UNICORN Binance WebSocket API’
# Project website: https://github.com/oliver-zehentleitner/unicorn-binance-websocket-api
# Documentation: https://oliver-zehentleitner.github.io/unicorn-binance-websocket-api
# PyPI: https://pypi.org/project/unicorn-binance-websocket-api/
#
# Author: Oliver Zehentleitner
#         https://about.me/oliver-zehentleitner
#
# Copyright (c) 2019-2020, Oliver Zehentleitner
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

import hashlib
import hmac
import json
import logging
import requests
import socket
import threading
import time


class BinanceWebSocketApiRestclient(object):
    def __init__(self, ubwa):
        self.ubwa = ubwa
        if self.ubwa.exchange == "binance.com":
            self.restful_base_uri = "https://api.binance.com/"
            self.path_userdata = "api/v3/userDataStream"
        elif self.ubwa.exchange == "binance.com-testnet":
            self.restful_base_uri = "https://testnet.binance.vision/"
            self.path_userdata = "api/v3/userDataStream"
        elif self.ubwa.exchange == "binance.com-margin":
            self.restful_base_uri = "https://api.binance.com/"
            self.path_userdata = "sapi/v1/userDataStream"
        elif self.ubwa.exchange == "binance.com-margin-testnet":
            self.restful_base_uri = "https://testnet.binance.vision/"
            self.path_userdata = "sapi/v1/userDataStream"
        elif self.ubwa.exchange == "binance.com-isolated_margin":
            self.restful_base_uri = "https://api.binance.com/"
            self.path_userdata = "sapi/v1/userDataStream/isolated"
        elif self.ubwa.exchange == "binance.com-isolated_margin-testnet":
            self.restful_base_uri = "https://testnet.binance.vision/"
            self.path_userdata = "sapi/v1/userDataStream/isolated"
        elif self.ubwa.exchange == "binance.com-futures":
            self.restful_base_uri = "https://fapi.binance.com/"
            self.path_userdata = "fapi/v1/listenKey"
        elif self.ubwa.exchange == "binance.com-futures-testnet":
            self.restful_base_uri = "https://testnet.binancefuture.com/"
            self.path_userdata = "fapi/v1/listenKey"
        elif self.ubwa.exchange == "binance.je":
            self.restful_base_uri = "https://api.binance.je/"
            self.path_userdata = "api/v1/userDataStream"
        elif self.ubwa.exchange == "binance.us":
            self.restful_base_uri = "https://api.binance.us/"
            self.path_userdata = "api/v1/userDataStream"
        elif self.ubwa.exchange == "jex.com":
            self.restful_base_uri = "https://www.jex.com/"
            self.path_userdata = "api/v1/userDataStream"

        self.api_key = False
        self.api_secret = False
        self.symbol = False
        self.listen_key = False
        self.unicorn_binance_websocket_api_version = self.ubwa.get_version()
        self.binance_api_status = self.ubwa.binance_api_status
        self.threading_lock = threading.Lock()

    def _get_signature(self, data):
        """
        Get the signature of 'data'

        :param data: the data you want to sign
        :type data: str

        :return: signature
        :rtype: str
        """
        try:
            hmac_signature = hmac.new(self.api_secret.encode('utf-8'), data.encode('utf-8'), hashlib.sha256)
            return hmac_signature.hexdigest()
        except AttributeError as error_msg:
            logging.critical(f"_get_signature({str(data)}) Error: {str(error_msg)}")
            return False

    def _request(self, method, path, query=False, data=False):
        """
        Do the request

        :param method: choose the method to use (post, put or delete)
        :type method: str
        :param path: choose the path to use
        :type path: str
        :param query: choose the query to use
        :type query: str
        :param data: the payload for the post method
        :type data: str
        :return: the response
        :rtype: str or False
        """
        requests_headers = {'Accept': 'application/json',
                            'User-Agent': 'oliver-zehentleitner/unicorn-binance-websocket-api/' +
                                          self.unicorn_binance_websocket_api_version,
                            'X-MBX-APIKEY': str(self.api_key)}
        if query is not False:
            uri = self.restful_base_uri + path + "?" + query
        else:
            uri = self.restful_base_uri + path
        try:
            if method == "post":
                if data is False:
                    request_handler = requests.post(uri, headers=requests_headers)
                else:
                    request_handler = requests.post(uri, headers=requests_headers, data=data)
            elif method == "put":
                request_handler = requests.put(uri, headers=requests_headers, data=data)
            elif method == "delete":
                request_handler = requests.delete(uri, headers=requests_headers)
            else:
                request_handler = False
        except requests.exceptions.ConnectionError as error_msg:
            logging.critical("BinanceWebSocketApiRestclient->_request() - error_msg: " + str(error_msg))
            return False
        except socket.gaierror as error_msg:
            logging.critical("BinanceWebSocketApiRestclient->_request() - error_msg: " + str(error_msg))
            return False
        if request_handler.status_code == "418":
            logging.critical("BinanceWebSocketApiRestclient->_request() received status_code 418 from binance! You got"
                             "banned from the binance api! Read this: https://github.com/binance-exchange/binance-"
                             "official-api-sphinx/blob/master/rest-api.md#limits")
        elif request_handler.status_code == "429":
            logging.critical("BinanceWebSocketApiRestclient->_request() received status_code 429 from binance! Back off"
                             "or you are going to get banned! Read this: https://github.com/binance-exchange/binance-"
                             "official-api-sphinx/blob/master/rest-api.md#limits")
        try:
            respond = request_handler.json()
        except json.decoder.JSONDecodeError as error_msg:
            logging.error("BinanceWebSocketApiRestclient->_request() - error_msg: " + str(error_msg))
            return False
        self.binance_api_status['weight'] = request_handler.headers.get('X-MBX-USED-WEIGHT')
        self.binance_api_status['timestamp'] = time.time()
        self.binance_api_status['status_code'] = request_handler.status_code
        request_handler.close()
        return respond

    def get_listen_key(self, stream_id=False, api_key=False, api_secret=False, symbol=False):
        """
        Request a valid listen_key from binance

        :param stream_id: provide a stream_id (only needed for userData Streams (acquiring a listenKey)
        :type stream_id: uuid
        :param api_key: provide a valid Binance API key
        :type api_key: str
        :param api_secret: provide a valid Binance API secret
        :type api_secret: str
        :param symbol: provide the symbol for isolated_margin user_data listen_key
        :type symbol: str
        :return: listen_key
        :rtype: str or False
        """
        logging.info(f"BinanceWebSocketApiRestclient->get_listen_key() symbol='{str(self.symbol)}' "
                     f"stream_id='{str(stream_id)}')")
        if stream_id is False:
            return False
        with self.threading_lock:
            self.init_vars(stream_id, api_key=api_key, api_secret=api_secret, symbol=symbol)
            method = "post"
            if self.ubwa.exchange == "binance.com-isolated_margin" or \
                    self.ubwa.exchange == "binance.com-isolated_margin-testnet":
                if self.symbol is False:
                    logging.critical("BinanceWebSocketApiRestclient->get_listen_key() Info: Parameter `symbol`"
                                     " is missing!")
                    return False
                else:
                    response = self._request(method, self.path_userdata, False, {'symbol': str(self.symbol)})
            else:
                response = self._request(method, self.path_userdata)
            try:
                self.listen_key = response['listenKey']
                return response
            except KeyError:
                return response
            except TypeError:
                return False

    def delete_listen_key(self, stream_id=False, api_key=False, api_secret=False, listen_key=False):
        """
        Delete a specific listen key

        :param stream_id: provide a stream_id (only needed for userData Streams (acquiring a listenKey)
        :type stream_id: uuid
        :param listen_key: the listenkey you want to delete
        :type listen_key: str or bool
        :return: the response
        :rtype: str or False
        """
        if stream_id is False:
            return False
        with self.threading_lock:
            self.init_vars(stream_id, api_key, api_secret, listen_key)
            return self.do_request(self.listen_key, "delete")

    def keepalive_listen_key(self, stream_id=False, api_key=False, api_secret=False, listen_key=False) -> object:
        """
        Ping a listenkey to keep it alive

        :param stream_id: provide a stream_id (only needed for userData Streams (acquiring a listenKey)
        :type stream_id: uuid
        :param listen_key: the listenkey you want to keepalive
        :type listen_key: str
        :return: the response
        :rtype: str or False
        """
        if stream_id is False:
            return False
        with self.threading_lock:
            self.init_vars(stream_id, api_key, api_secret, listen_key)
            return self.do_request(self.listen_key, "keepalive")

    def do_request(self, listen_key, action=False):
        """
        Do a request!

        :param listen_key: the listenkey you want to keepalive
        :type listen_key: str
        :param action: choose "delete" or "keepalive"
        :type action: str
        :return: the response
        :rtype: str or False
        """
        if action == "keepalive":
            if self.ubwa.show_secrets_in_logs is True:
                logging.info("BinanceWebSocketApiRestclient->keepalive_listen_key(" + str(listen_key) + ")")
            else:
                logging.info("BinanceWebSocketApiRestclient->keepalive_listen_key(" + str(self.ubwa.replaced_secrets_text)
                             + ")")
            method = "put"
            try:
                return self._request(method, self.path_userdata, False, {'listenKey': str(listen_key)})
            except KeyError:
                return False
            except TypeError:
                return False
        elif action == "delete":
            if self.ubwa.show_secrets_in_logs is True:
                logging.info("BinanceWebSocketApiRestclient->delete_listen_key(" + str(listen_key) + ")")
            else:
                logging.info("BinanceWebSocketApiRestclient->delete_listen_key(" + str(self.ubwa.replaced_secrets_text)
                             + ")")
            method = "delete"
            try:
                return self._request(method, self.path_userdata, False, {'listenKey': str(listen_key)})
            except KeyError:
                return False
            except TypeError:
                return False
        else:
            return False

    def init_vars(self, stream_id, api_key=False, api_secret=False, symbol=False, listen_key=False):
        """
        set default values and load values from stream_list

        :param stream_id: provide a stream_id (only needed for userData Streams (acquiring a listenKey)
        :type stream_id: uuid
        :param api_key: provide a valid Binance API key
        :type api_key: str
        :param api_secret: provide a valid Binance API secret
        :type api_secret: str
        :param symbol: provide the symbol for isolated_margin user_data listen_key
        :type symbol: str
        :param listen_key: provide the listen_key
        :type listen_key: str
        :return: bool
        """
        try:
            if api_key is False:
                self.api_key = self.ubwa.stream_list[stream_id]['api_key']
            else:
                self.api_key = api_key
            if api_secret is False:
                self.api_secret = self.ubwa.stream_list[stream_id]['api_secret']
            else:
                self.api_secret = api_secret
            if symbol is False:
                self.symbol = self.ubwa.stream_list[stream_id]['symbols']
            else:
                self.symbol = symbol
            if listen_key is False:
                self.listen_key = False
            else:
                self.listen_key = listen_key
        except KeyError as error_msg:
            logging.error(f"BinanceWebSocketApiRestclient->init_vars() failed with TypeError: {str(error_msg)}")
            return False
        return True
