CREATE TABLE IF NOT EXISTS ev_user (
  id uuid NOT NULL PRIMARY KEY,
  name varchar(255) NOT NULL,
  age int NOT NULL,
  created_at timestamptz NOT NULL
);

INSERT INTO
  ev_user (id, name, age, created_at)
VALUES
  (
    '6dc14965-7a4c-47fa-899b-88a47935d47a',
    'Jewel93',
    34,
    '2023-10-10T11:30:30.100Z'
  ),
  (
    '72ed1afd-1e2a-4591-a003-e76a772e1d09',
    'Winnifred37',
    21,
    '2023-10-10T11:30:30.100Z'
  ),
  (
    '97539238-b72e-443e-988e-45d513b1a94c',
    'Wyman.Huel',
    21,
    '2023-10-10T11:30:30.100Z'
  ),
  (
    'b105b39f-3275-45f0-8fc2-a58d4c47e576',
    'Zander56',
    45,
    '2022-11-12T14:55:22.040Z'
  ),
  (
    '5e3e5261-ba41-4236-8ef8-62565381ef3b',
    'Duncan69',
    67,
    '2022-11-12T14:55:22.053Z'
  ),
  (
    'bfe0576a-75d4-47bc-a708-117c53941f53',
    'Hattie.Ratke19',
    31,
    '2023-10-05T21:17:25.560Z'
  ),
  (
    '3107a31c-244c-43cb-baf0-a04150cde165',
    'Iliana.Heaney88',
    17,
    '2023-10-21T2:46:16.380Z'
  ),
  (
    'eb78fe37-f79d-4cfa-be97-a849ea5434d9',
    'Cordelia.Kshlerin',
    48,
    '2023-08-30T07:50:33.39Z'
  ),
  (
    '510b026f-b25f-4484-9119-3515de00168e',
    'Horacio33',
    55,
    '2023-08-31T08:35:56.20Z'
  ),
  (
    'd9d47126-1985-4945-9ff0-c9463c916782',
    'Cullen_Leffler',
    29,
    '2024-01-31T11:30:30.492'
  );
  