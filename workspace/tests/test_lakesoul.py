# SPDX-FileCopyrightText: LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa


class B(pa.dataset.Dataset):
    pass


def test_lakesoul():
    print("in test")
