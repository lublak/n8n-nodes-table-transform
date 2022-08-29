import { set } from 'lodash';
import { IDataObject, INodeExecutionData } from 'n8n-workflow';

/**
 * Get all table column names from the execution data.
 *
 * @param   {INodeExecutionData[]} items - The exection data.
 * @returns {string[]}                   The column names.
 */
function getTableColumns(items: INodeExecutionData[]): string[] {
  const length = items.length;
  if (length > 0) {
    const tableColumns = Object.keys(items[0].json);
    for (let itemIndex = 1; itemIndex < length; itemIndex++) {
      for (const col of Object.keys(items[itemIndex].json)) {
        if (!tableColumns.includes(col)) tableColumns.push(col);
      }
    }
    return tableColumns;
  } else {
    return [];
  }
}

/**
 * Transpose the exection data.
 *
 * @param   {INodeExecutionData[]} items - The exection data.
 * @returns {INodeExecutionData[]}       The transposed exection data.
 */
export function transpose(items: INodeExecutionData[]): INodeExecutionData[] {
  let data: IDataObject;
  return getTableColumns(items).map((key) => {
    data = {};
    items.forEach((item, index) => (data[index] = item.json[key]));
    return { json: data };
  }) as INodeExecutionData[];
}

/**
 * Navigates in a cell of the exection data.
 *
 * @param   {INodeExecutionData[]} items     - The exection data.
 * @param   {number}               row       - The row number.
 * @param   {string}               col       - The column name.
 * @param   {boolean}              expand    - Whether to extend the data with the previous data.
 * @param   {boolean}              loopArray - Whether the data, if they represent an array, are
 *   automatically processed as rows.
 * @returns {INodeExecutionData[]}           The navigated exection data.
 * @throws            If row out of bounds.
 */
export function navigateIntoCell(
  items: INodeExecutionData[],
  row: number,
  col: string,
  expand: boolean,
  loopArray: boolean,
): INodeExecutionData[] {
  if (row < 0) throw new Error('The row has to be set to at least 0 or higher!');
  if (items.length - 1 < row) throw new Error('The row index is higher then rows length!');

  const rowData = items[row];
  let colData = rowData.json[col];
  if (colData === undefined) {
    colData = rowData.json[parseInt(col)];
  }
  if (loopArray && Array.isArray(colData)) {
    let newItem: INodeExecutionData;
    return colData.map((item) => {
      newItem = {
        json: {},
      };

      if (expand) Object.assign(newItem.json, rowData.json);
      if (typeof item === 'object') Object.assign(newItem.json, item);
      else newItem.json[col] = item;

      if (rowData.binary !== undefined) {
        newItem.binary = {};
        Object.assign(newItem.binary, rowData.binary);
      }
      return newItem;
    });
  } else {
    const newItem: INodeExecutionData = {
      json: {},
    };
    if (expand) Object.assign(newItem.json, rowData.json);
    if (typeof colData === 'object') Object.assign(newItem.json, colData);
    else newItem.json[col] = colData;

    if (rowData.binary !== undefined) {
      newItem.binary = {};
      Object.assign(newItem.binary, rowData.binary);
    }
    return [newItem];
  }
}

/**
 * Navigates in a column of the exection data.
 *
 * @param   {INodeExecutionData[]}      items            - The exection data.
 * @param   {(index: number) => string} colByIndex       - A function that returns the column name
 *   for an index.
 * @param   {Function}                  expandbyIndex    - A function that returns for an index
 *   whether to extend the data with the previous data.
 * @param   {Function}                  loopArrayByIndex - A function that returns for an index
 *   whether the data, if they represent an array, are automatically processed as rows.
 * @returns {INodeExecutionData[]}                       The navigated exection data.
 */
export function navigateIntoCol(
  items: INodeExecutionData[],
  colByIndex: (index: number) => string,
  expandbyIndex: (index: number) => boolean,
  loopArrayByIndex: (index: number) => boolean,
): INodeExecutionData[] {
  let rowData: INodeExecutionData;
  let colData: unknown;
  let newItem: INodeExecutionData;
  const newItems: INodeExecutionData[] = [];
  for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
    const loopArray = loopArrayByIndex(itemIndex);
    const expand = expandbyIndex(itemIndex);
    const col = colByIndex(itemIndex);

    rowData = items[itemIndex];
    colData = rowData.json[col] as unknown;
    if (colData === undefined) {
      colData = rowData.json[parseInt(col)] as unknown;
    }

    if (loopArray && Array.isArray(colData)) {
      for (const item of colData) {
        newItem = {
          json: {},
        };

        if (expand) Object.assign(newItem.json, rowData.json);
        if (typeof item === 'object') Object.assign(newItem.json, item);
        else newItem.json[col] = item as IDataObject;

        if (rowData.binary !== undefined) {
          newItem.binary = {};
          Object.assign(newItem.binary, rowData.binary);
        }
        newItems.push(newItem);
      }
    } else {
      newItem = {
        json: {},
      };

      if (expand) Object.assign(newItem.json, rowData.json);
      if (typeof colData === 'object') Object.assign(newItem.json, colData);
      else newItem.json[col] = colData as IDataObject;

      if (rowData.binary !== undefined) {
        newItem.binary = {};
        Object.assign(newItem.binary, rowData.binary);
      }
      newItems.push(newItem);
    }
  }
  return newItems;
}

/**
 * Navigates in a row of the exection data.
 *
 * @param   {INodeExecutionData[]} items            - The exection data.
 * @param   {number}               row              - The row number.
 * @param   {string}               col              - The column name.
 * @param   {Function}             expandbyIndex    - A function that returns for an index whether
 *   to extend the data with the previous data.
 * @param   {Function}             loopArrayByIndex - A function that returns for an index whether
 *   the data, if they represent an array, are automatically processed as rows.
 * @returns {INodeExecutionData[]}                  The navigated exection data.
 * @throws                   If row out of bounds.
 */
export function navigateIntoRow(
  items: INodeExecutionData[],
  row: number,
  col: string,
  expandbyIndex: (index: number) => boolean,
  loopArrayByIndex: (index: number) => boolean,
): INodeExecutionData[] {
  if (row < 0) throw new Error('The row has to be set to at least 0 or higher!');
  if (items.length - 1 < row) throw new Error('The row index is higher then rows length!');

  const loopArray = loopArrayByIndex(row);
  const expand = expandbyIndex(row);

  const rowData: INodeExecutionData = items[row];
  const newItems: INodeExecutionData[] = [];

  let newItem: INodeExecutionData;
  for (const [col, colData] of Object.entries(rowData.json)) {
    if (loopArray && Array.isArray(colData)) {
      for (const item of colData) {
        newItem = {
          json: {},
        };

        if (expand)
          Object.assign(
            newItem.json,
            items.map((rowItem) => rowItem.json[col]),
          );
        if (typeof item === 'object') Object.assign(newItem.json, item);
        else newItem.json[col] = item;

        if (rowData.binary !== undefined) {
          newItem.binary = {};
          Object.assign(newItem.binary, rowData.binary);
        }
        newItems.push(newItem);
      }
    } else {
      newItem = {
        json: {},
      };

      if (expand)
        Object.assign(
          newItem.json,
          items.map((rowItem) => rowItem.json[col]),
        );
      if (typeof colData === 'object') Object.assign(newItem.json, colData);
      else newItem.json[col] = colData;

      if (rowData.binary !== undefined) {
        newItem.binary = {};
        Object.assign(newItem.binary, rowData.binary);
      }
      newItems.push(newItem);
    }
  }
  return newItems;
}

/**
 * Demotes the header the exection data.
 *
 * @param   {INodeExecutionData[]} items - The exection data.
 * @returns {INodeExecutionData[]}       The exection data with demoted header.
 */
export function demoteHeader(items: INodeExecutionData[]): INodeExecutionData[] {
  if (items.length === 0) return items;
  const keys = getTableColumns(items);
  let newItem: INodeExecutionData;

  const newItems: INodeExecutionData[] = items.map((item) => {
    newItem = {
      json: {},
    };

    if (item.binary !== undefined) {
      newItem.binary = {};
      Object.assign(newItem.binary, item.binary);
    }

    keys.forEach((key, index) => {
      newItem.json[index] = item.json[key];
    });
    return newItem;
  });
  newItems.unshift({ json: { ...keys } });
  return newItems;
}

/**
 * Promotes the header the exection data.
 *
 * @param   {INodeExecutionData[]} items - The exection data.
 * @returns {INodeExecutionData[]}       The exection data with promoted header.
 */
export function promoteHeader(items: INodeExecutionData[]): INodeExecutionData[] {
  const shiftedItems = [...items];
  const header = shiftedItems.shift();
  if (header === undefined) return items;

  const keys = getTableColumns(items);

  let newItem: INodeExecutionData;
  let counter;
  let newkey;
  const headerValues = Object.values(header.json);

  return shiftedItems.map((item) => {
    newItem = {
      json: {},
    };

    if (item.binary !== undefined) {
      newItem.binary = {};
      Object.assign(newItem.binary, item.binary);
    }

    for (let index = 0; index < keys.length; index++) {
      const key = keys[index];
      if (header.json[key])
        newItem.json[header.json[key] as string] = item.json[key]
          ? (item.json[key] as string)
          : null;
      else {
        newkey = index.toString();
        if (headerValues.includes(newkey)) {
          counter = 0;
          do {
            newkey = `${index}_${counter}`;
            counter++;
          } while (headerValues.includes(newkey));
        }
        header.json[key] = newkey;
        newItem.json[newkey] = item.json[key] ? (item.json[key] as string) : null;
      }
    }
    return newItem;
  });
}

/**
 * Counts based on countType rows, cols or cells.
 *
 * @param   {INodeExecutionData[]} items          - The exection data.
 * @param   {string}               countType      - What to count.
 * @param   {string}               destinationKey - In which column name the count value should be stored.
 * @returns {INodeExecutionData[]}                A single element with the count value.
 * @throws                 If countType is not rows, cols or cells.
 */
export function count(
  items: INodeExecutionData[],
  countType: 'rows' | 'cols' | 'cells',
  destinationKey: string,
): INodeExecutionData[] {
  const data: IDataObject = {};
  switch (countType) {
    case 'rows':
      set(data, destinationKey, items.length);
      break;
    case 'cols':
      if (items.length > 0) {
        set(data, destinationKey, getTableColumns(items).length);
      } else {
        set(data, destinationKey, 0);
      }
      break;
    case 'cells': {
      const length = items.length;
      if (length > 0) {
        set(data, destinationKey, getTableColumns(items).length * length);
      } else {
        set(data, destinationKey, 0);
      }
      break;
    }
    default:
      throw new Error('rows, cols or cells are valid options');
  }
  return [{ json: data }];
}
