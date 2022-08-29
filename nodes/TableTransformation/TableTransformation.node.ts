import { IExecuteFunctions } from 'n8n-core';
import {
  INodeExecutionData,
  INodeParameters,
  INodeType,
  INodeTypeDescription,
  NodeOperationError,
} from 'n8n-workflow';

import {
  count,
  demoteHeader,
  navigateIntoCell,
  navigateIntoCol,
  navigateIntoRow,
  promoteHeader,
  transpose,
} from './TableTransformation.node.functions';

/**
 * A node which allows you to transform the table.
 */
export class TableTransform implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'TableTransform',
    name: 'tableTransformation',
    icon: 'file:TableTransform.svg',
    group: ['transformation'],
    version: 1,
    description: 'Allows you to manipulate string values.',
    defaults: {
      name: 'TableTransform',
      color: '#772244',
    },
    inputs: ['main'],
    outputs: ['main'],
    properties: [
      {
        displayName: 'Transformations',
        name: 'transformations',
        placeholder: 'Add Transformation',
        type: 'fixedCollection',
        typeOptions: {
          multipleValues: true,
          sortable: true,
        },
        description: 'The transformations for the data sources',
        default: {},
        options: [
          {
            name: 'transformation',
            displayName: 'Transformation',
            values: [
              {
                displayName: 'Action',
                name: 'action',
                type: 'options',
                options: [
                  {
                    name: 'Count',
                    value: 'count',
                    description: 'Count the rows, cols or cells',
                    action: 'Count the rows cols or cells',
                  },
                  {
                    name: 'Demote Header',
                    value: 'demoteHeader',
                    description: 'Move header as row and creates a new header',
                    action: 'Move header as row and creates a new header',
                  },
                  {
                    name: 'Navigate',
                    value: 'navigate',
                    description: 'Navigate in a nested table',
                    action: 'Navigate in a nested table',
                  },
                  {
                    name: 'Promote Header',
                    value: 'promoteHeader',
                    description:
                      'First row will use as header and the current header will be removed',
                    action: 'First row will use as header and the current header will be removed',
                  },
                  {
                    name: 'Transpose',
                    value: 'transpose',
                    description: 'Swap rows with columns',
                    action: 'Swap rows with columns',
                  },
                ],
                default: 'transpose',
              },
            ],
          },
          {
            displayName: 'Type',
            name: 'navigateType',
            type: 'options',
            displayOptions: {
              show: {
                action: ['navigate'],
              },
            },
            options: [
              {
                name: 'Row',
                value: 'row',
              },
              {
                name: 'Column',
                value: 'col',
              },
              {
                name: 'Cell',
                value: 'cell',
              },
            ],
            default: 'cell',
            description: 'In which way you want to navigate',
          },
          {
            displayName: 'Row',
            name: 'row',
            type: 'number',
            displayOptions: {
              show: {
                action: ['navigate'],
                navigateType: ['row', 'cell'],
              },
            },
            typeOptions: {
              minValue: 0,
            },
            default: 0,
            description: 'Row index',
          },
          {
            displayName: 'Col',
            name: 'col',
            type: 'string',
            displayOptions: {
              show: {
                action: ['navigate'],
                navigateType: ['col', 'cell'],
              },
            },
            default: '',
            required: true,
            description: 'Col name',
          },
          {
            displayName: 'Expand',
            name: 'expand',
            type: 'boolean',
            displayOptions: {
              show: {
                action: ['navigate'],
              },
            },
            default: true,
            required: true,
            description: 'Whether to extend the data with the previous data',
          },
          {
            displayName: 'Loop Array',
            name: 'loopArray',
            type: 'boolean',
            displayOptions: {
              show: {
                action: ['navigate'],
              },
            },
            default: true,
            required: true,
            description:
              'Whether the data, if they represent an array, are automatically processed as rows',
          },
          {
            displayName: 'Type',
            name: 'countType',
            type: 'options',
            displayOptions: {
              show: {
                action: ['count'],
              },
            },
            options: [
              {
                name: 'Rows',
                value: 'rows',
              },
              {
                name: 'Columns',
                value: 'cols',
              },
              {
                name: 'Cells',
                value: 'cells',
              },
            ],
            default: 'rows',
            description: 'What exactly to count',
          },
          {
            displayName: 'Destination Key',
            name: 'destinationKey',
            type: 'string',
            displayOptions: {
              show: {
                action: ['count'],
              },
            },
            default: 'data',
            required: true,
            placeholder: 'data',
            description:
              'The name the JSON key to copy data to. It is also possibleto define deep keys by using dot-notation like for example:"level1.level2.newKey".',
          },
        ],
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    let returnData = this.getInputData();
    //let returnData: INodeExecutionData[] = items.map(item => {
    //  const newItem: INodeExecutionData = {
    //    json: JSON.parse(JSON.stringify(item.json)) as IDataObject,
    //  };
    //  if (item.binary !== undefined) {
    //    newItem.binary = {};
    //    Object.assign(newItem.binary, item.binary);
    //  }
    //  return newItem;
    //});

    for (const transformation of (this.getNodeParameter('transformations.transformation', 0, []) as
      | INodeParameters[]
      | null) ?? []) {
      switch (transformation.action) {
        case 'transpose':
          returnData = transpose(returnData);
          break;
        case 'navigate':
          switch (transformation.navigateType) {
            case 'row':
              returnData = navigateIntoRow(
                returnData,
                this.getNodeParameter('row', 0) as number,
                this.getNodeParameter('col', 0) as string,
                (index) => this.getNodeParameter('expand', index) as boolean,
                (index) => this.getNodeParameter('loopArray', index) as boolean,
              );
              break;
            case 'col':
              returnData = navigateIntoCol(
                returnData,
                (index) => this.getNodeParameter('col', index) as string,
                (index) => this.getNodeParameter('expand', index) as boolean,
                (index) => this.getNodeParameter('loopArray', index) as boolean,
              );
              break;
            case 'cell':
              returnData = navigateIntoCell(
                returnData,
                this.getNodeParameter('row', 0) as number,
                this.getNodeParameter('col', 0) as string,
                this.getNodeParameter('expand', 0) as boolean,
                this.getNodeParameter('loopArray', 0) as boolean,
              );
              break;
            default:
              throw new NodeOperationError(this.getNode(), 'row, col or cell are valid options');
          }
          break;
        case 'demoteHeader':
          returnData = demoteHeader(returnData);
          break;
        case 'promoteHeader':
          returnData = promoteHeader(returnData);
          break;
        case 'count': {
          const countType = this.getNodeParameter('countType', 0);
          if (countType != 'rows' && countType != 'cols' && countType != 'cells')
            throw new NodeOperationError(this.getNode(), 'rows, cols or cells are valid options');
          returnData = count(
            returnData,
            countType,
            this.getNodeParameter('destinationKey', 0) as string,
          );
          break;
        }
        default:
          throw new NodeOperationError(this.getNode(), 'transpose or navigate are valid options');
      }
    }

    return this.prepareOutputData(returnData);
  }
}
