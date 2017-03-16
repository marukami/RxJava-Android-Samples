package com.morihacky.android.rxjava.fragments;

import static android.text.TextUtils.isEmpty;
import static android.util.Patterns.EMAIL_ADDRESS;

import android.os.Bundle;
import android.support.annotation.ColorRes;
import android.support.annotation.Nullable;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.EditText;
import android.widget.TextView;

import butterknife.BindView;
import butterknife.ButterKnife;
import butterknife.Unbinder;

import com.jakewharton.rxbinding2.widget.RxTextView;
import com.morihacky.android.rxjava.R;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.functions.Predicate;
import io.reactivex.subscribers.DisposableSubscriber;
import timber.log.Timber;

public class FormValidationCombineLatestFragment extends BaseFragment {

  @BindView(R.id.btn_demo_form_valid)
  TextView _btnValidIndicator;

  @BindView(R.id.demo_combl_email)
  EditText _email;

  @BindView(R.id.demo_combl_password)
  EditText _password;

  @BindView(R.id.demo_combl_num)
  EditText _number;

  private DisposableSubscriber<Boolean> _disposableObserver = null;
  private Flowable<CharSequence> _emailChangeObservable;
  private Flowable<CharSequence> _numberChangeObservable;
  private Flowable<CharSequence> _passwordChangeObservable;
  private Unbinder unbinder;

  @Override
  public View onCreateView(
      LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
    View layout = inflater.inflate(R.layout.fragment_form_validation_comb_latest, container, false);
    unbinder = ButterKnife.bind(this, layout);

    _emailChangeObservable =
        RxTextView.textChanges(_email).toFlowable(BackpressureStrategy.LATEST);
    _passwordChangeObservable =
        RxTextView.textChanges(_password).toFlowable(BackpressureStrategy.LATEST);
    _numberChangeObservable =
        RxTextView.textChanges(_number).toFlowable(BackpressureStrategy.LATEST);

    _combineLatestEvents();

    return layout;
  }

  @Override
  public void onDestroyView() {
    super.onDestroyView();
    unbinder.unbind();
    _disposableObserver.dispose();
  }

  private void _combineLatestEvents() {

    _disposableObserver =
        new DisposableSubscriber<Boolean>() {
          @Override
          public void onNext(Boolean formValid) {
            @ColorRes
            int color = formValid ? R.color.blue : R.color.gray;
            _btnValidIndicator.setBackgroundColor(getResources().getColor(color));
          }

          @Override
          public void onError(Throwable e) {
            Timber.e(e, "there was an error");
          }

          @Override
          public void onComplete() {
            Timber.d("completed");
          }
        };

    Predicate<CharSequence> notEmpty = (x -> !isEmpty(x));
    // Wait until each field is dirt
    Flowable<Boolean> dirtyCheckFlowable = Flowable.combineLatest(
        _emailChangeObservable.filter(notEmpty).take(1),
        _passwordChangeObservable.filter(notEmpty).take(1),
        _numberChangeObservable.filter(notEmpty).take(1),
        (email, password, number) -> !isEmpty(email) && !isEmpty(number) && !isEmpty(password));

    Flowable.combineLatest(
        _emailChangeObservable,
        _passwordChangeObservable,
        _numberChangeObservable,
        dirtyCheckFlowable, // Just here to trigger the first event
        (newEmail, newPassword, newNumber, __ ) -> {
          boolean emailValid = !isEmpty(newEmail) && EMAIL_ADDRESS.matcher(newEmail).matches();
          if (!emailValid) {
            _email.setError("Invalid Email!");
          }

          boolean passValid = !isEmpty(newPassword) && newPassword.length() > 8;
          if (!passValid) {
            _password.setError("Invalid Password!");
          }

          boolean numValid = !isEmpty(newNumber);
          if (numValid) {
            int num = Integer.parseInt(newNumber.toString());
            numValid = num > 0 && num <= 100;
          }
          if (!numValid) {
            _number.setError("Invalid Number!");
          }

          return emailValid && passValid && numValid;
        })
        .subscribe(_disposableObserver);
  }
}
