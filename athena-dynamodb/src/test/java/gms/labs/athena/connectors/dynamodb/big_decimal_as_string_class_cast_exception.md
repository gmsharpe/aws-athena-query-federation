
`DBTableUtils.peekTableForSchema(...)` determines that the column contains 'big_decimal' values.

* DynamoDb only has 'number' defined for schema.  how is BigDecimal obtained in schema?


```
# From BlockUtils:355

  case DECIMAL:
        DecimalVector dVector = ((DecimalVector) vector);
        if (value instanceof Double) {
            BigDecimal bdVal = new BigDecimal((double) value);
            bdVal = bdVal.setScale(dVector.getScale(), RoundingMode.HALF_UP);
            dVector.setSafe(pos, bdVal);
        }
        else {
            // todo - gmsharpe (12/22/2020) this is where the ClassCastException takes place
            BigDecimal scaledValue = ((BigDecimal) value).setScale(dVector.getScale(), RoundingMode.HALF_UP);
            ((DecimalVector) vector).setSafe(pos, scaledValue);
        }
        break;
```
