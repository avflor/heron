//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.streamlet;


import java.io.Serializable;

/**
 * Transformation depending on Windowing pass on the window/key information
 * using this class
 */
public class KeyedWindow<T> implements Serializable {
  private static final long serialVersionUID = 4193319775040181971L;
  private T key;
  private Window window;
  public KeyedWindow(T key, Window window) {
    this.key = key;
    this.window = window;
  }
  public T getKey() {
    return key;
  }
  public Window getWindow() {
    return window;
  }
  @Override
  public String toString() {
    return "{ Key: " + String.valueOf(key) + " Window: " + window.toString() + " }";
  }
}
