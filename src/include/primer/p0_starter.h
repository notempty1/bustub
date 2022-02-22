//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) {
    rows_ = rows;
    cols_ = cols;
    this->linear_ = new T[rows * cols];
    //printf("rows = %d", rows);
  }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() = default;
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
   int rows_;
   int cols_;
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    rows_ = rows;
    cols_ = cols;
    this->data_ = new T*[rows];
    for ( int row = 0; row < rows; row++){
      this->data_[row] = new T[cols];
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return this->rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return this->cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    if(0 <= i && i < rows_ && 0 <= j && j < cols_){
      return this->data_[i][j];
    }else{
      throw Exception(ExceptionType::OUT_OF_RANGE, "Out Of Range");
    }
    //throw NotImplementedException{"RowMatrix::GetElement() not implemented."};
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if(0 <= i && i < rows_ && 0 <= j && j < cols_){
      this->data_[i][j] = val;
    }else{
      throw Exception(ExceptionType::OUT_OF_RANGE, "Out Of Range");
    }
    
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    int s_length = int(source.size());
    int m_length = rows_ * cols_ ;
    if(s_length == m_length) {
      int i = 0;
      for ( int row = 0; row < rows_; row++ ) {
        for ( int col = 0; col < cols_; col++ ) {
          this->SetElement(row, col, source[i]);
          i++;
        }
      }
    }else{
      throw Exception(ExceptionType::OUT_OF_RANGE, "Numeric value out of range.");
    }
    //throw NotImplementedException{"RowMatrix::FillFrom() not implemented."};
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override = default;

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    auto matrix1 = std::make_unique<RowMatrix<int>>(3, 3);
    *matrix1 = *matrixA;
    int rows = matrix1->GetRowCount();
    int cols = matrix1->GetColumnCount();
    for (int row = 0; row < rows; row ++){
      for (int col = 0; col < cols; col ++){
        int a = matrixA->GetElement(row, col);
        int b = matrixB->GetElement(row, col);
        matrix1->SetElement(row, col, a+b);
      }
    }
    return matrix1;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    if ( matrixA->GetColumnCount() != matrixB->GetRowCount()){
      printf("a_columns = %d, b_rows = %d", matrixA->GetColumnCount(), matrixB->GetRowCount());
      throw Exception(ExceptionType::OUT_OF_RANGE, "out of range");
    } 
    int a_rows = matrixA->GetRowCount();
    int a_columns = matrixA->GetColumnCount();
    int b_rows = matrixB->GetRowCount();
    int b_columns = matrixB->GetColumnCount();
    
    auto martix = std::make_unique<RowMatrix<int>>(a_rows, b_columns);
    for( int i = 0; i < a_rows; i++){
      for ( int j = 0; j < b_columns; j++){
        int a[a_columns] = {0};
        int b[b_rows] = {0};
        for (int col = 0; col < a_columns; col ++){
          a[col] = matrixA->GetElement(i, col);
        }
        for (int row = 0; row < b_rows; row ++){
          b[row] = matrixB->GetElement(row, j);
        }
        int ele = 0;
        if (a_columns == b_rows){
          int cnt = a_columns;
          for (int i = 0; i < cnt; i++){
            ele += a[i] * b[i];
          }
        }else {
          printf("a_columns = %d b_rows = %d", a_columns,b_rows);
          throw Exception(ExceptionType::OUT_OF_RANGE, "out_of_range");
        }
        martix->SetElement(i, j, ele);
      }
    }
    printf("end\n");
    // TODO(P0): Add implementation
    return martix;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    // TODO(P0): Add implementation
    return std::unique_ptr<RowMatrix<T>>(nullptr);
  }
};
}  // namespace bustub
