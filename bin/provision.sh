#!/usr/bin/env bash

curl -X POST https://sandbox-rest.ably.io/apps \
 -H "Content-Type: application/json" \
 --data '
  {
    "keys":[
      {},
      {
        "capability": "{ \"*\":[\"subscribe\",\"publish\"] }"
      },
      {
        "capability": "{ \"private\":[\"subscribe\",\"publish\"], \"chat\":[\"presence\"] }"
      }
    ],
    "namespaces":[
      { "id":"persisted", "persisted":true },
      { "id":"chat", "persisted":true }
    ],
    "channels":[
      {
        "name":"chat",
        "presence":[
          { "clientId":"John", "data":"john@test.com" },
          { "clientId":"Dave", "data":"dave@test.com" }
        ]
      }
    ]
  }'
